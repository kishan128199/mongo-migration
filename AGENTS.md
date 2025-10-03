# Repository Guidelines

## Project Structure & Module Organization
- `src/cli.js` is the CLI entry point that wires commands to the loaders and runner.
- `src/config.js` centralizes environment parsing; keep configuration logic there.
- `src/lib/` holds shared helpers (`logger`, `migrationLoader`, `migrationRunner`, `mongo`); add reusable services here.
- `src/migrations/` contains ordered scripts such as `0001-sample-user-profile.js`; copy the template when adding files.
- `templates/migration.js` provides the migration scaffold.
- `.env` (copied from `.env.example`) stores connection secrets and must stay out of version control.

## Build, Test, and Development Commands
- `npm install` installs project dependencies.
- `npm run migrate -- list` enumerates migrations using the current configuration.
- `npm run migrate -- run <name>` executes a migration; pair with `--dry-run` to simulate writes.
- `npm run migrate:dry -- run <name>` forces dry-run mode through environment variables.
- `DEBUG=true npm run migrate -- run <name>` prints stack traces for easier debugging.

## Coding Style & Naming Conventions
- Target Node 18+, CommonJS modules, and 2-space indentation; keep multi-line literals trailing-comma friendly.
- Use camelCase for symbols and kebab-case with numeric prefixes for migration filenames (`0002-new-schema.js`).
- Keep migrations idempotent, log intent via the shared logger, and respect the injected `dryRun` guard.
- Re-run `npm run migrate -- list` after renaming files to confirm the loader resolves them.

## Testing Guidelines
- Lean on dry runs plus Mongo shell queries to validate transformations before writes; capture evidence in PR notes.
- Add automated checks beside the code (`src/lib/__tests__/migrationRunner.test.js`) using Jest or Node Test Runner when introduced.
- Cover batching, failure paths, and configuration overrides; store fixtures under `test/fixtures` if needed.

## Commit & Pull Request Guidelines
- Git history is not yet formalized, so adopt Conventional Commit subjects (`feat: cache migration loader`) for clarity.
- Mention issue IDs, configuration changes, and representative CLI output in commit bodies or PR descriptions.
- PRs should summarize intent, list validation commands, and include screenshots only when visual tooling is relevant.

## Configuration & Security Tips
- Keep `.env` local; document required keys instead of committing secrets.
- Prefer read-only source credentials for dry runs and rotate target credentials regularly.
- Store ad-hoc secrets in your shell profile, not in scripts or migration files.
