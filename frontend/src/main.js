/**
 * main.js — точка входа Vite-сборки (Phase 3: managers extracted)
 *
 * Порядок загрузки важен:
 *   1. AuthManager   — monkey-patches window.fetch, должен идти первым
 *   2. ClusterSelector — не зависит от Auth, но использует fetch (уже патченый)
 *   3. WsManager     — зависит от getApiBase(), clusterData, renderAll() из index.html
 *   4. NotifManager  — зависит только от clusterData (проверяется через check())
 *
 * На Phase 3 все page-модули ещё живут в index.html.
 * На Phase 4 они будут импортированы здесь.
 *
 * ПРИМЕЧАНИЕ: window.* assignments оставлены в каждом manager-файле намеренно —
 * inline onclick="ClusterSelector.selectContour(...)" в HTML продолжают работать
 * до полного завершения Phase 4 (замены inline handlers).
 */

import './managers/AuthManager.js';
import './managers/ClusterSelector.js';
import './managers/WsManager.js';
import './managers/NotifManager.js';

// Phase 4 page imports (добавлять по одному):
// import './pages/docs.js';
// import './pages/topology.js';
// import './pages/overview.js';
// import './pages/nodes.js';
// import './pages/diagnostics.js';
// import './pages/recovery.js';
// import './pages/maintenance.js';
// import './pages/settings.js';

// DOMContentLoaded init
document.addEventListener('DOMContentLoaded', function() {
    if (window.AuthManager) window.AuthManager.init();
});
