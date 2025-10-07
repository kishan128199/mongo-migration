# Repository Guidelines

## Project Structure & Module Organization
Core CLI lives in `src/cli.js`, handling flags and delegating to helpers in `src/lib/`. Shared utilities live there as well: `migrationLoader.js` for discovery, `migrationRunner.js` for orchestration, and `mongo.js` for connections. Runtime configuration sits in `src/config.js`. Place executable migrations in `src/migrations/` with zero-padded prefixes (e.g., `0007-user_profiles.js`) to preserve order. Use `templates/migration.js` as a starting point when adding steps, updating inline docs to match the scenario.

## Build, Test, and Development Commands
Run `npm install` after cloning or pulling. Use `npm run migrate -- list` to inspect registered migrations. Execute one with `npm run migrate -- run <id>` and supply `--dry-run` to simulate writes. `npm run migrate:dry -- run <id>` forces dry-run mode via `DRY_RUN=true`. Prefix commands with `DEBUG=true` to emit verbose logs while diagnosing issues.

## Coding Style & Naming Conventions
Target Node 18+ with CommonJS modules (`module.exports`). Keep two-space indentation, trailing commas on multi-line literals, and camelCase identifiers. Prefer async/await in migrations and scope logging with `logger.child({ migrationId })`. Ensure migrations are idempotent and guard against partial writes before toggling dry-run off.

## Testing Guidelines
No automated test harness ships with the repo. Instead, validate with repeated dry runs against representative data sets, logging counters like `processed`, `matchedCount`, and `modifiedCount`. Document any manual verification steps in the migration comments so reviewers can reproduce checks. Abort early if a write would occur while still in dry-run mode.

## Commit & Pull Request Guidelines
Follow Conventional Commits (`feat:`, `fix:`, `chore:`). Keep commits focused on a single migration or helper change and include doc/template updates when relevant. Pull requests should explain the business goal, summarize dry-run results, link related tickets, and call out manual verification performed. Attach console excerpts when they clarify behavior.

## Security & Configuration Tips
Never commit `.env` values. Adjust connection defaults in `src/config.js` and pass secrets through environment variables or secret managers. Use `MIGRATIONS_DIR` overrides for experiments instead of editing production paths. Audit logs with `DEBUG=true` before promoting migrations to live environments, and rotate credentials if accidentally exposed.
