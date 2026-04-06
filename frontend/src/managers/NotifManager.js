/**
 * NotifManager — браузерные push-уведомления о деградации кластера.
 *
 * Публичное API:
 *   NotifManager.toggle(bool)   — включить/выключить уведомления
 *   NotifManager.check(data)    — проверить clusterData и отправить уведомление если нужно
 *
 * Триггеры уведомлений:
 *   1. wsrep_cluster_status переходит из 'Primary' в non-Primary
 *   2. wsrep_flow_control_paused пересекает порог 0.1 снизу вверх
 *   3. Нода уходит в offline
 *
 * DOM-зависимости:
 *   #notifToggle (checkbox)
 *
 * Глобальные функции (опционально, вызываются через typeof guard):
 *   toast(), addLog()
 */
var NotifManager = (function() {
    var _enabled    = false;
    var _prevStatus = null;
    var _prevFC     = 0;
    var LS_KEY      = 'galera_notif_enabled';

    function _load() {
        try { _enabled = localStorage.getItem(LS_KEY) === '1'; } catch(e) {}
        var cb = document.getElementById('notifToggle');
        if (cb) cb.checked = _enabled;
    }

    function toggle(on) {
        if (on && Notification.permission === 'default') {
            Notification.requestPermission().then(function(p) {
                if (p === 'granted') {
                    _enabled = true;
                    _save();
                    _send('\u2705 Galera Orchestrator', '\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432\u043a\u043b\u044e\u0447\u0435\u043d\u044b. \u041e\u043f\u043e\u0432\u0435\u0449\u0443 \u043f\u0440\u0438 \u0434\u0435\u0433\u0440\u0430\u0434\u0430\u0446\u0438\u0438 \u043a\u043b\u0430\u0441\u0442\u0435\u0440\u0430.');
                } else {
                    _enabled = false;
                    var cb = document.getElementById('notifToggle');
                    if (cb) cb.checked = false;
                    if (typeof toast === 'function') toast('warning', '\u0411\u0440\u0430\u0443\u0437\u0435\u0440 \u0437\u0430\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u043b \u0443\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f. \u0420\u0430\u0437\u0440\u0435\u0448\u0438\u0442\u0435 \u0432 \u043d\u0430\u0441\u0442\u0440\u043e\u0439\u043a\u0430\u0445 \u0431\u0440\u0430\u0443\u0437\u0435\u0440\u0430.');
                }
            });
        } else if (on && Notification.permission === 'granted') {
            _enabled = true;
            _save();
            _send('\u2705 Galera Orchestrator', '\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0432\u043a\u043b\u044e\u0447\u0435\u043d\u044b.');
        } else if (on && Notification.permission === 'denied') {
            _enabled = false;
            var cb = document.getElementById('notifToggle');
            if (cb) cb.checked = false;
            if (typeof toast === 'function') toast('warning', '\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u044f \u0437\u0430\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u043d\u044b \u0432 \u0431\u0440\u0430\u0443\u0437\u0435\u0440\u0435.');
        } else {
            _enabled = false;
            _save();
        }
    }

    function _save() {
        try { localStorage.setItem(LS_KEY, _enabled ? '1' : '0'); } catch(e) {}
        var cb = document.getElementById('notifToggle');
        if (cb) cb.checked = _enabled;
    }

    function _send(title, body) {
        if (Notification.permission !== 'granted') return;
        try { new Notification(title, { body: body, icon: '/favicon.ico', tag: 'galera' }); }
        catch(e) {}
    }

    function check(data) {
        if (!_enabled || !data || !data.nodes) return;

        var status = (data.nodes[0] && data.nodes[0].metrics &&
                      data.nodes[0].metrics.wsrep_cluster_status) || 'unknown';
        var fc = 0;
        data.nodes.forEach(function(n) {
            if (n.metrics) fc = Math.max(fc, parseFloat(n.metrics.wsrep_flow_control_paused) || 0);
        });

        // 1. non-Primary alert
        if (status !== 'Primary' && status !== 'unknown' && _prevStatus === 'Primary') {
            _send('\u26a0\ufe0f \u041a\u043b\u0430\u0441\u0442\u0435\u0440 \u2192 non-Primary',
                  '\u0414\u0435\u0433\u0440\u0430\u0434\u0430\u0446\u0438\u044f: wsrep_cluster_status = ' + status + '. \u0417\u0430\u043f\u0438\u0441\u044c \u0437\u0430\u0431\u043b\u043e\u043a\u0438\u0440\u043e\u0432\u0430\u043d\u0430.');
            if (typeof addLog === 'function') addLog('ERROR', '\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435: \u043a\u043b\u0430\u0441\u0442\u0435\u0440 \u2192 ' + status);
        }

        // 2. Flow Control high
        if (fc > 0.1 && _prevFC <= 0.1) {
            _send('\u23f3 Flow Control \u0432\u044b\u0441\u043e\u043a\u0438\u0439',
                  'fc_paused = ' + fc.toFixed(3) + ' (\u043f\u043e\u0440\u043e\u0433 0.1). \u041f\u0440\u043e\u0431\u043b\u0435\u043c\u0430 \u043f\u0440\u043e\u0438\u0437\u0432\u043e\u0434\u0438\u0442\u0435\u043b\u044c\u043d\u043e\u0441\u0442\u0438.');
            if (typeof addLog === 'function') addLog('WARN', '\u0423\u0432\u0435\u0434\u043e\u043c\u043b\u0435\u043d\u0438\u0435: fc_paused = ' + fc.toFixed(3));
        }

        // 3. Node offline
        data.nodes.forEach(function(n) {
            if (!n.online && n._prevOnline !== false) {
                _send('\u274c \u041d\u043e\u0434\u0430 \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0430: ' + (n.name || n.id),
                      (n.host || '') + ' \u2014 ' + (n.error || '\u043e\u0448\u0438\u0431\u043a\u0430 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f'));
            }
            n._prevOnline = n.online;
        });

        _prevStatus = status;
        _prevFC     = fc;
    }

    document.addEventListener('DOMContentLoaded', _load);
    if (document.readyState !== 'loading') _load();

    return { toggle: toggle, check: check };
})();

window.NotifManager = NotifManager;
