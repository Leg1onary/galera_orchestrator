# Migration Guide: v1 → v2

## Status
- [x] Repo structure created
- [x] Vite config
- [x] Docker + docker-compose
- [x] Manager stubs (WsManager, NotifManager, AuthManager, ClusterSelector)
- [x] **Phase 3: Managers migrated to `frontend/src/managers/*.js`**
- [ ] Phase 2: CSS extracted to separate files
- [ ] Phase 4: Pages migrated to modules
- [ ] Phase 5: Legacy index.html removed

## Step-by-step plan

### Phase 1 — Setup (done)
```bash
npm install
npm run dev   # starts Vite dev server on :5173, proxies /api → :8000
```

### Phase 2 — Extract CSS
Copy CSS blocks from `v1/frontend/index.html` into:
- `frontend/src/styles/base.css`       ← :root, reset, body
- `frontend/src/styles/themes.css`     ← [data-theme="dark/light"]
- `frontend/src/styles/components.css` ← everything else

### Phase 3 — Migrate managers ✅ DONE (commit c1074b6)
All four managers extracted from monolithic `index.html` into:
```
frontend/src/managers/
├── AuthManager.js      — JWT auth, monkey-patches window.fetch
├── ClusterSelector.js  — contour (test/prod) and cluster switching
├── WsManager.js        — WebSocket with heartbeat + exponential reconnect
└── NotifManager.js     — browser push notifications (3 triggers)
```
Entry point: `frontend/src/main.js`

**Key decisions:**
- Each manager ends with `window.X = X` — backward compat with inline `onclick=` handlers in HTML
- `AuthManager` is imported first — it monkey-patches `window.fetch` before other managers make requests
- `typeof toast === 'function'` guards in ClusterSelector and NotifManager — safe if toast not yet initialized
- WsManager global deps (`clusterData`, `renderAll`, etc.) still live in `index.html` until Phase 4

### Phase 4 — Migrate pages (one at a time)
Recommended order (simplest → most complex):
1. `docs.js`         — static content, no API calls
2. `topology.js`     — SVG render, read-only
3. `overview.js`     — KPIs + node cards
4. `nodes.js`        — detail grid + search
5. `diagnostics.js`  — processlist, garbd, cnf compare
6. `recovery.js`     — Bootstrap Wizard
7. `maintenance.js`  — Maintenance Wizard
8. `settings.js`     — forms + yaml preview

Each page module exports a `{ init(), refresh() }` interface.
Uncomment the corresponding import in `frontend/src/main.js` as each page is migrated.

### Phase 5 — Docker build
```bash
npm run build          # outputs to frontend/dist/
docker compose up -d   # build image and start
```

## Dev workflow
```
Terminal 1:  uvicorn backend.main:app --reload --port 8000
Terminal 2:  npm run dev   (Vite on :5173, proxies /api)
Browser:     http://localhost:5173
```

## Notes
- `window.App`, `window.WsManager` etc. are kept during migration
  so legacy `onclick="App.go('overview')"` in HTML still works
- Remove `window.*` assignments only after all inline handlers are replaced
- nodes.yaml is volume-mounted in Docker — never baked into the image
