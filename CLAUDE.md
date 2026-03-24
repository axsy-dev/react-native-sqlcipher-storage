# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

React Native SQLCipher Storage — encrypted SQLite (SQLCipher) bindings for React Native with a unified JavaScript API across iOS, Android, Windows, and Electron. Forked from react-native-sqlite-storage, adapted from Cordova-sqlcipher-adapter.

## Commands

- **Lint:** `npm run lint` (ESLint with Prettier via `@react-native/eslint-config`)
- **Run example (Android):** `npm run start:android`
- **Run example (iOS):** `npm run start:ios`

## Architecture

The library exposes a single JS API that delegates to platform-specific native modules via React Native's bridge (or Electron IPC).

### JavaScript layer (src/)

- **`src/sqlite.core.js`** — Core runtime: `SQLitePlugin` (database handle), `SQLitePluginTransaction` (transaction queue with lock management), and `SQLiteFactory` (open/delete databases). Dispatches calls to native modules via `plugin.exec()`. On web/Electron, routes to `window.sqliteapi` (exposed by the preload script); on native platforms, routes to `NativeModules.SQLite`.
- **`src/index.js`** — Promise/callback adapter. Wraps `sqlite.core.js` classes so that calling `enablePromise(true)` switches the API from callback-style to Promise-style. Exports a singleton `SQLiteFactory` instance as the default export.
- **`src/index.d.ts`** — TypeScript type definitions.

### Platform-specific native implementations

Each platform implements the same four operations: `open`, `close`, `delete`, `backgroundExecuteSqlBatch`.

- **iOS** (`ios/SQLite.m`) — Objective-C `RCTBridgeModule`. Links against SQLCipher via `SQLITE_HAS_CODEC=1` preprocessor flag and CocoaPods `SQLCipher ~>4.0`.
- **Android** (`android/src/main/java/com/axsy/`) — Java React Native module. Uses `net.zetetic:android-database-sqlcipher:4.4.2` with a thread pool and `ConcurrentHashMap` for concurrent database management.
- **Windows** (`windows/`) — C++ WinRT implementation requiring OpenSSL build.
- **Electron** (`electron/`) — Three-file IPC architecture:
  - `main.js` — Main process: `SQLite` class wraps `@journeyapps/sqlcipher` with async wrappers (`AsyncDatabase`, `AsyncStatement`). Registers IPC handlers via `db.main.init()`.
  - `preload.js` — Context bridge: exposes `window.sqliteapi` to renderer via `contextBridge.exposeInMainWorld()`. Initialize with `db.preload.init()`.
  - `renderer.js` — Thin proxy: exports `electronAPI` object that forwards calls to `window.sqliteapi`.

### Package entry points

```
"."        → ./src/index.js       (main library)
"./preload" → ./electron/preload.js (Electron preload script)
"./main"    → ./electron/main.js    (Electron main process)
```

### Transaction model

Transactions are queued per-database in `txLocks` (keyed by db name). Each lock tracks a FIFO queue and an `inProgress` flag. Locked transactions automatically wrap statements in `BEGIN`/`COMMIT`/`ROLLBACK`. Statements within a transaction are batched and sent to the native layer via `backgroundExecuteSqlBatch`.

## Project conventions

- ES Modules (`"type": "module"` in package.json)
- No TypeScript compilation — source is plain JS with JSDoc type annotations and a separate `.d.ts` file
- Prettier 2.8.8 formatting enforced through ESLint
