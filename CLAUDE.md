# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

React Native SQLCipher Storage — encrypted SQLite (SQLCipher) bindings for React Native with a unified JavaScript API across iOS, Android, Windows, and Electron. Forked from react-native-sqlite-storage, adapted from Cordova-sqlcipher-adapter.

## Commands

- **Lint:** `npm run lint` (ESLint with Prettier via `@react-native/eslint-config`)
- **Run example (Android):** `npm run start:android`
- **Run example (iOS):** `npm run start:ios`
- **Android tests** (run from `example/mobile/android`, after `npm install` in `example/mobile`):
  - JVM: `./gradlew :react-native-sqlcipher-storage:testDebugUnitTest`
  - Device, 4.4.2 parity (`LegacyParityTest`): `./gradlew :react-native-sqlcipher-storage:connectedDebugAndroidTest`. It runs on every connected device, so run it on both a 4 KB-page and a 16 KB-page emulator.

## Architecture

The library exposes a single JS API that delegates to platform-specific native modules via React Native's bridge (or Electron IPC).

### JavaScript layer (src/)

- **`src/sqlite.core.js`** — Core runtime: `SQLitePlugin` (database handle), `SQLitePluginTransaction` (transaction queue with lock management), and `SQLiteFactory` (open/delete databases). Dispatches calls to native modules via `plugin.exec()`. On web/Electron, routes to `window.sqliteapi` (exposed by the preload script); on native platforms, routes to `NativeModules.SQLite`.
- **`src/index.js`** — Promise/callback adapter. Wraps `sqlite.core.js` classes so that calling `enablePromise(true)` switches the API from callback-style to Promise-style. Exports a singleton `SQLiteFactory` instance as the default export.
- **`src/index.d.ts`** — TypeScript type definitions.

### Platform-specific native implementations

Each platform implements the same four operations: `open`, `close`, `delete`, `backgroundExecuteSqlBatch`.

- **iOS** (`ios/SQLite.m`) — Objective-C `RCTBridgeModule`. Links against SQLCipher via `SQLITE_HAS_CODEC=1` preprocessor flag and CocoaPods `SQLCipher ~>4.0`.
- **Android** (`android/src/main/java/com/axsy/`) — Java React Native module. Uses `net.zetetic:sqlcipher-android` (package `net.zetetic.database.sqlcipher`) with a thread pool and `ConcurrentHashMap` for concurrent database management.
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

## Android SQLCipher upgrades

- A Gradle-only bump between `android-database-sqlcipher` (`net.sqlcipher.*`) and `sqlcipher-android` (`net.zetetic.database.sqlcipher.*`) does not compile; the plugin has to be ported with it (09bf838 was, and was reverted in cd88bf0).
- `openDatabase` must pass the JS `key` as its plain UTF-8 bytes. That is what 4.4.2 handed to `sqlite3_key`, so every existing install's PBKDF2 key depends on it; any other encoding opens nothing, and the plugin then retries with `cipher_migrate`.
- sqlcipher-android does not behave like 4.4.2 by default: `SQLiteConnection.open()` sets `synchronous=NORMAL`, `journal_size_limit=10000` and a 2500 ms busy timeout, and its default error handler never deletes a corrupt file. `openLegacy`, `LEGACY_ERROR_HANDLER` and `legacyMessage` in `SQLitePlugin.java` restore 4.4.2's settings, delete-on-`SQLITE_CORRUPT` and error text; recheck them against the new library's source on every bump. A wrong key is `SQLITE_NOTADB`, not corruption, in both, so it never deletes.
- Check every ABI's `libsqlcipher.so` for 16 KB pages: LOAD `p_align` >= 0x4000 is not enough, `GNU_RELRO` must not end mid 16 KB page on a page a writable LOAD also maps. react-app's `scripts/android/check-16kb-alignment.ts` checks both.
