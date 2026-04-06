/**
 * AuthManager — JWT-авторизация с monkey-patch fetch.
 *
 * Публичное API:
 *   AuthManager.init()        — вызывается один раз на DOMContentLoaded
 *   AuthManager.logout()      — разлогиниться
 *   AuthManager.getToken()    — вернуть текущий Bearer-токен (или '')
 *   AuthManager.isEnabled()   — true если бекенд требует авторизацию
 *
 * Зависимости (глобальные, резолвятся в рантайме):
 *   window.fetch (monkey-patched здесь)
 *   DOM: #loginOverlay, #loginForm, #loginUsername, #loginPassword,
 *        #loginBtn, #loginError, #logoutBtn
 */
var AuthManager = (function() {
    var LS_TOKEN = 'galera_auth_token';
    var _authEnabled = false;

    function getToken() {
        try { return localStorage.getItem(LS_TOKEN) || ''; } catch(e) { return ''; }
    }
    function setToken(t) {
        try { localStorage.setItem(LS_TOKEN, t); } catch(e) {}
    }
    function clearToken() {
        try { localStorage.removeItem(LS_TOKEN); } catch(e) {}
    }

    // Monkey-patch global fetch to always attach the Bearer token
    // and redirect to login on 401.
    var _origFetch = window.fetch;
    window.fetch = function(input, init) {
        init = init || {};
        init.headers = init.headers || {};
        var token = getToken();
        if (token) {
            init.headers['Authorization'] = 'Bearer ' + token;
        }
        return _origFetch.call(window, input, init).then(function(resp) {
            if (resp.status === 401 && _authEnabled) {
                clearToken();
                showLogin();
            }
            return resp;
        });
    };

    function showLogin() {
        var overlay = document.getElementById('loginOverlay');
        if (overlay) overlay.classList.add('active');
        var uField = document.getElementById('loginUsername');
        if (uField) uField.focus();
    }
    function hideLogin() {
        var overlay = document.getElementById('loginOverlay');
        if (overlay) overlay.classList.remove('active');
    }

    function showError(msg) {
        var el = document.getElementById('loginError');
        if (!el) return;
        el.textContent = msg;
        el.classList.add('visible');
    }
    function clearError() {
        var el = document.getElementById('loginError');
        if (el) el.classList.remove('visible');
    }

    function doLogin(username, password) {
        var btn = document.getElementById('loginBtn');
        if (btn) { btn.disabled = true; btn.classList.add('loading'); }
        clearError();

        _origFetch('/api/auth/login', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ username: username, password: password })
        })
        .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
        .then(function(res) {
            if (res.ok && res.data.token) {
                setToken(res.data.token);
                hideLogin();
                showLogoutBtn();
            } else {
                showError(res.data.detail || 'Неверный логин или пароль');
            }
        })
        .catch(function() { showError('Ошибка соединения с сервером'); })
        .finally(function() {
            if (btn) { btn.disabled = false; btn.classList.remove('loading'); }
        });
    }

    function logout() {
        _origFetch('/api/auth/logout', { method: 'POST',
            headers: { 'Authorization': 'Bearer ' + getToken() } });
        clearToken();
        showLogin();
        clearError();
    }

    function showLogoutBtn() {
        var btn = document.getElementById('logoutBtn');
        if (btn) btn.style.display = '';
    }

    function init() {
        var form = document.getElementById('loginForm');
        if (form) {
            form.addEventListener('submit', function(e) {
                e.preventDefault();
                var u = (document.getElementById('loginUsername') || {}).value || '';
                var p = (document.getElementById('loginPassword') || {}).value || '';
                doLogin(u.trim(), p);
            });
        }
        ['loginUsername','loginPassword'].forEach(function(id) {
            var el = document.getElementById(id);
            if (el) el.addEventListener('keydown', function(e) {
                if (e.key === 'Enter') { if (form) form.dispatchEvent(new Event('submit')); }
            });
        });

        _origFetch('/api/auth/status')
            .then(function(r) { return r.json(); })
            .then(function(d) {
                _authEnabled = !!d.enabled;
                if (!_authEnabled) return;

                showLogoutBtn();
                var token = getToken();
                if (!token) { showLogin(); return; }

                _origFetch('/api/auth/me', {
                    headers: { 'Authorization': 'Bearer ' + token }
                }).then(function(r) {
                    if (!r.ok) { clearToken(); showLogin(); }
                }).catch(function() { showLogin(); });
            })
            .catch(function() { /* backend offline — let app handle it */ });
    }

    return { init: init, logout: logout, getToken: getToken, isEnabled: function() { return _authEnabled; } };
})();

window.AuthManager = AuthManager;
