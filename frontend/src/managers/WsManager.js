/**
 * WsManager — WebSocket-соединение с кластером, авто-реконнект, heartbeat, fallback на polling.
 *
 * Публичное API:
 *   WsManager.enable()       — включить WS-режим и подключиться
 *   WsManager.disable()      — отключиться и вернуться на HTTP polling
 *   WsManager.connect()      — (переподключиться вручную)
 *   WsManager.isConnected()  — true если сокет открыт
 *
 * Глобальные зависимости (резолвятся в рантайме):
 *   getApiBase()         — функция, возвращает базовый URL API
 *   clusterData          — глобальный объект состояния кластера
 *   _dataMode            — 'mock' | 'real'
 *   _mergeConfigNodes()  — обновить config-поля нод после WS push
 *   _lastUpdateStr       — строка времени последнего обновления
 *   _tickCount           — счётчик тиков
 *   _pollTimer           — таймер HTTP-поллинга
 *   renderAll()          — перерисовать весь UI
 *   startAutoRefresh()   — запустить HTTP polling
 *   addLog()             — добавить запись в лог-панель
 *   SstMonitor.check()   — проверить статус SST
 *
 * DOM: #wsIndicator
 */
var WsManager = (function() {
    var _ws          = null;
    var _reconnectMs = 1000;   // начальная задержка реконнекта
    var _maxRecoMs   = 30000;  // максимальная задержка
    var _reconnTimer = null;
    var _active      = false;
    var _indicator   = null;

    function _getWsUrl() {
        var base = getApiBase();
        var wsBase = base.replace(/^http/, 'ws');
        return wsBase + '/ws/cluster';
    }

    function _setIndicator(state) {
        if (!_indicator) _indicator = document.getElementById('wsIndicator');
        if (!_indicator) return;
        _indicator.style.display = 'inline-block';
        var colors = {
            connected:  'var(--color-success)',
            connecting: 'var(--color-warning)',
            error:      'var(--color-error)',
            off:        'var(--color-text-faint)'
        };
        _indicator.style.background = colors[state] || colors.off;
        _indicator.title = 'WebSocket: ' + state;
    }

    function _onMessage(evt) {
        try {
            var msg = JSON.parse(evt.data);
            if (!msg || !msg.type) return;

            if (msg.type === 'status') {
                var rawNodes = msg.nodes || [];
                clusterData.nodes = rawNodes.map(function(nd) {
                    var state = nd.wsrep_local_state_comment || (nd.online === false ? 'Offline' : 'Unknown');
                    return {
                        id: nd.id || nd.name, name: nd.name || nd.id,
                        host: nd.host || '', port: nd.port || 3306,
                        ssh_port: nd.ssh_port || 22,
                        online: nd.online !== false, error: nd.error || null, state: state,
                        metrics: {
                            wsrep_cluster_status:      nd.wsrep_cluster_status || '—',
                            wsrep_cluster_size:        String(nd.wsrep_cluster_size || '0'),
                            wsrep_connected:           nd.wsrep_connected || 'OFF',
                            wsrep_ready:               nd.wsrep_ready || 'OFF',
                            wsrep_local_state_comment: state,
                            wsrep_local_recv_queue:    String(nd.wsrep_local_recv_queue || '0'),
                            wsrep_local_send_queue:    String(nd.wsrep_local_send_queue || '0'),
                            wsrep_flow_control_paused: String(nd.wsrep_flow_control_paused || '0.000'),
                            wsrep_cert_deps_distance:  String(nd.wsrep_cert_deps_distance || '0.00'),
                            wsrep_last_committed:      String(nd.wsrep_last_committed || nd.wsrep_local_commits || '0'),
                            wsrep_local_cert_failures: String(nd.wsrep_local_cert_failures || '0'),
                            wsrep_bf_aborts:           String(nd.wsrep_bf_aborts || '0'),
                            wsrep_cluster_conf_id:     String(nd.wsrep_cluster_conf_id || '0'),
                            wsrep_cluster_state_uuid:  nd.wsrep_cluster_state_uuid || '—',
                            wsrep_replicated_bytes:    nd.wsrep_replicated_bytes || '—',
                            wsrep_received_bytes:      nd.wsrep_received_bytes || '—',
                        },
                        read_only:    nd.read_only    || false,
                        sst_progress: nd.sst_progress || 0,
                        sst_method:   nd.sst_method   || null,
                    };
                });
                if (msg.arbitrators && _dataMode === 'real') clusterData.arbitrators = msg.arbitrators;
                _mergeConfigNodes();
                if (msg.cluster_name || msg.environment) {
                    clusterData.cluster = clusterData.cluster || {};
                    if (msg.cluster_name) clusterData.cluster.name = msg.cluster_name;
                    if (msg.environment)  clusterData.cluster.environment = msg.environment;
                }
                clusterData.timestamp = new Date();
                _lastUpdateStr = clusterData.timestamp.toLocaleTimeString('ru-RU');
                _tickCount++;
                renderAll();
                SstMonitor.check();

            } else if (msg.type === 'event') {
                addLog(msg.level || 'INFO', '[WS] ' + (msg.message || ''));
            }
            // msg.type === 'pong' — heartbeat ответ, no-op
        } catch(e) {
            // invalid JSON — ignore
        }
    }

    function connect() {
        if (!_active) return;
        if (_ws && (_ws.readyState === WebSocket.CONNECTING || _ws.readyState === WebSocket.OPEN)) return;

        _setIndicator('connecting');
        var url = _getWsUrl();

        try {
            _ws = new WebSocket(url);
        } catch(e) {
            _fallback('WS create error: ' + e.message);
            return;
        }

        _ws.onopen = function() {
            _reconnectMs = 1000;
            _setIndicator('connected');
            addLog('INFO', 'WebSocket подключён: ' + url);
            if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }
            _startHeartbeat();
        };

        _ws.onmessage = _onMessage;

        _ws.onerror = function() {
            _setIndicator('error');
        };

        _ws.onclose = function(evt) {
            _stopHeartbeat();
            if (!_active) { _setIndicator('off'); return; }
            _setIndicator('error');
            addLog('WARN', 'WebSocket отключён (code=' + evt.code + '). Реконнект через ' + (_reconnectMs/1000) + 'с...');
            if (!_pollTimer) startAutoRefresh();
            _reconnTimer = setTimeout(function() { connect(); }, _reconnectMs);
            _reconnectMs = Math.min(_reconnectMs * 2, _maxRecoMs);
        };
    }

    var _hbTimer = null;
    function _startHeartbeat() {
        _stopHeartbeat();
        _hbTimer = setInterval(function() {
            if (_ws && _ws.readyState === WebSocket.OPEN) { _ws.send('ping'); }
        }, 20000);
    }
    function _stopHeartbeat() {
        if (_hbTimer) { clearInterval(_hbTimer); _hbTimer = null; }
    }

    function _fallback(reason) {
        addLog('WARN', 'WS недоступен (' + reason + ') — fallback на HTTP polling');
        _setIndicator('error');
        _active = false;
        if (!_pollTimer) startAutoRefresh();
    }

    function enable() {
        _active = true;
        connect();
    }

    function disable() {
        _active = false;
        _stopHeartbeat();
        if (_reconnTimer) { clearTimeout(_reconnTimer); _reconnTimer = null; }
        if (_ws) { try { _ws.close(1000, 'disabled'); } catch(e){} _ws = null; }
        _setIndicator('off');
        startAutoRefresh();
    }

    function isConnected() {
        return _ws && _ws.readyState === WebSocket.OPEN;
    }

    return { enable: enable, disable: disable, connect: connect, isConnected: isConnected };
})();

window.WsManager = WsManager;
