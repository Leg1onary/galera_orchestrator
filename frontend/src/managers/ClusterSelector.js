/**
 * ClusterSelector — управление контурами (test/prod) и выбором кластера.
 *
 * Публичное API:
 *   ClusterSelector.load()                  — загрузить список контуров с /api/contours
 *   ClusterSelector.setMockMode(bool)        — скрыть/показать панель контуров
 *   ClusterSelector.selectContour(str)       — переключить контур ('test'|'prod')
 *   ClusterSelector.selectCluster(idxStr)    — выбрать кластер по индексу (строка)
 *   ClusterSelector._inject(data, isMock)    — инжектировать данные без HTTP (для mock)
 *
 * DOM-зависимости:
 *   #contourBtnTest, #contourBtnProd, #clusterSelect, #contourBar
 *
 * Глобальные функции (опционально, вызываются через typeof guard):
 *   fetchRealData(), toast(), addLog()
 */
var ClusterSelector = (function() {
    var _contour      = 'test';
    var _clusterIdx   = 0;
    var _contours     = {};   // {test: ['cluster-1', 'cluster-2'], prod: []}
    var _isMockMode   = true;

    function _updateUI() {
        var btnTest = document.getElementById('contourBtnTest');
        var btnProd = document.getElementById('contourBtnProd');
        if (btnTest) {
            var isTest = _contour === 'test';
            btnTest.className = 'btn btn-sm' + (isTest ? ' active' : ' btn-ghost');
            btnTest.style.background = isTest ? 'var(--color-primary)' : '';
            btnTest.style.color      = isTest ? '#fff' : '';
        }
        if (btnProd) {
            var isProd = _contour === 'prod';
            btnProd.className = 'btn btn-sm' + (isProd ? ' active' : ' btn-ghost');
            btnProd.style.background = isProd ? '#6daa45' : '';
            btnProd.style.color      = isProd ? '#fff' : '';
        }

        var sel = document.getElementById('clusterSelect');
        if (sel) {
            var clusters = _contours[_contour] || [];
            sel.innerHTML = clusters.length
                ? clusters.map(function(name, i) {
                    return '<option value="'+i+'"'+(i===_clusterIdx?' selected':'')+'>'+name+'</option>';
                  }).join('')
                : '<option value="0">Нет кластеров</option>';
        }

        // Contour bar: hide in mock mode (mock data is topology-agnostic)
        var bar = document.getElementById('contourBar');
        if (bar) bar.style.display = _isMockMode ? 'none' : 'flex';
    }

    function load() {
        return fetch('/api/contours')
            .then(function(r) { return r.json(); })
            .then(function(d) {
                _contours   = d.contours   || {};
                var sel     = d.selection  || {};
                _contour    = sel.contour      || 'test';
                _clusterIdx = sel.cluster_index|| 0;
                _updateUI();
            })
            .catch(function() {});
    }

    function setMockMode(isMock) {
        _isMockMode = isMock;
        _updateUI();
    }

    function selectContour(contour) {
        var clusters = _contours[contour];
        if (!clusters || clusters.length === 0) {
            if (_contours.hasOwnProperty(contour)) {
                if (typeof toast === 'function') toast('warn', 'Контур ' + contour.toUpperCase() + ' не имеет кластеров');
            } else {
                if (typeof toast === 'function') toast('warn', 'Контур ' + contour.toUpperCase() + ' не найден в nodes.yaml');
            }
            return;
        }
        _contour    = contour;
        _clusterIdx = 0;
        _updateUI();
        _persist();
    }

    function selectCluster(idxStr) {
        var idx = parseInt(idxStr, 10);
        if (isNaN(idx)) return;
        _clusterIdx = idx;
        _persist();
    }

    function _persist() {
        fetch('/api/contours/select', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({contour: _contour, cluster_index: _clusterIdx})
        })
        .then(function(r) { return r.json(); })
        .then(function(d) {
            if (d.ok && typeof fetchRealData === 'function') {
                fetchRealData();
                if (typeof addLog === 'function')
                    addLog('INFO', 'Активный кластер: ' + d.cluster_name + ' (' + _contour.toUpperCase() + ')');
            }
        })
        .catch(function() {});
    }

    function _inject(data, isMock) {
        _contours   = data.contours   || {};
        var sel     = data.selection  || {};
        _contour    = sel.contour      || 'test';
        _clusterIdx = sel.cluster_index || 0;
        _isMockMode = !!isMock;
        _updateUI();
    }

    return { load: load, setMockMode: setMockMode, selectContour: selectContour,
             selectCluster: selectCluster, _inject: _inject };
})();

window.ClusterSelector = ClusterSelector;
