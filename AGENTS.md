# Repository Guidelines

## Project Structure & Module Organization
- `src/cli.js` exposes the migration CLI, reading runtime flags and delegating work to helpers in `src/lib/`.
- `src/config.js` centralizes `.env` loading; adjust connection defaults there when introducing new environments.
- Place executable migration scripts in `src/migrations/`, using zero-padded numeric prefixes plus a slug (e.g., `0007-user_profiles.js`) to preserve execution order.
- `templates/migration.js` seeds new migrations; copy it when scaffolding additional steps.

## Build, Test, and Development Commands
- `npm install` — install dependencies after cloning or pulling.
- `npm run migrate -- list` — print discoverable migrations using the configured directory.
- `npm run migrate -- run <id>` — execute a specific migration; combine with `--dry-run` to simulate writes.
- `npm run migrate:dry -- run <id>` — force dry-run mode via `DRY_RUN=true` for safety when testing.
- Prefix commands with `DEBUG=true` to surface verbose stack traces during troubleshooting.

## Coding Style & Naming Conventions
- Use modern Node 18+ features with CommonJS modules; exports remain `module.exports` for consistency.
- Follow the existing two-space indentation, trailing commas on multi-line literals, and descriptive camelCase identifiers for variables and functions.
- Prefer async/await inside migrations, reuse `logger.child()` for scoped logging, and keep migration files idempotent so they can be rerun.

## Testing Guidelines
- No automated test runner ships with this scaffold; validate migrations via repeated dry runs against sampled data.
- Log key counters (`processed`, `matchedCount`) and guard against partial writes before toggling out of dry-run mode.
- Document any manual verification steps in the migration description to aid reviewers.

## Commit & Pull Request Guidelines
- Follow Conventional Commits (`feat:`, `fix:`, `chore:`) as seen in history to clarify intent and aid changelog generation.
- Keep commits focused on a single migration or helper change, including updated docs/templates alongside code.
- PRs should explain the business goal, outline data validation performed, and link to related tickets; attach dry-run output snippets when available.

## Configuration & Security Notes
- Never commit `.env` contents; rotate credentials if they are exposed outside secure channels.
- Use `MIGRATIONS_DIR` overrides for experimental branches rather than editing production paths.
- Share connection URIs through secret managers, and audit logs with `DEBUG=true` before promoting migrations to live environments.
