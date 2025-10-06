# Repository Guidelines

## Project Structure & Module Organization
The CLI entry point lives in `src/cli.js`, which wires configuration, logging, and migration execution. Runtime helpers sit under `src/lib/` (`logger.js`, `migrationLoader.js`, `migrationRunner.js`, `mongo.js`)—keep new utilities colocated there so the CLI stays slim. Configuration defaults are centralized in `src/config.js`. Active migration scripts reside in `src/migrations/` and follow the ordered pattern `NNNN-description.js`; copy `templates/migration.js` when creating a new step so shared logging and argument structure stay consistent.

## Build, Test, and Development Commands
- `npm install` – install dependencies before running the CLI.
- `npm run migrate -- list` – enumerate migrations discovered in `src/migrations/`.
- `npm run migrate -- run 0001-auth` – execute a migration with full writes enabled.
- `npm run migrate:dry -- run 0001-auth` – force `DRY_RUN` mode for rehearsal runs regardless of `.env` settings.
Set `.env` or `ENV_PATH` to point at connection credentials before invoking these commands.

## Coding Style & Naming Conventions
Code is CommonJS, formatted with 2-space indentation, semicolons, and single-quoted strings. Prefer `const`/`let` over `var`, async/await over promise chains, and keep functions pure except for database work. When adding modules, export a single responsibility per file and surface named functions for easier test coverage. Migration files should export `{ id, description, up }` and keep log messages succinct (`logger.info('copying users batch')`).

## Testing Guidelines
Automated tests are not yet present; rely on dry runs plus targeted MongoDB queries to validate transformations. Use `npm run migrate:dry` with representative data, inspect logs, and confirm document shapes in both source and target databases before enabling writes. If you add automated coverage, prefer lightweight integration tests that run migrations against seeded in-memory Mongo instances and document the setup in this guide.

## Commit & Pull Request Guidelines
Write imperative, present-tense commit subjects under 60 characters (e.g., `Add migration runner batching`). Each commit should bundle related changes across code and docs. Pull requests must describe the migration intent, list verification steps (`npm run migrate:dry -- run <name>` outputs, Mongo shell spot checks), and note configuration updates. Include links to related tickets and mention any follow-up tasks. Avoid committing secrets; keep `.env` local.

## Configuration & Security Tips
Secrets load via `dotenv`; never check `.env` into git. Use `.env.example` as the canonical template and document new variables there. `DEBUG=true` enables full stack traces—only enable it locally. Confirm `BATCH_SIZE` and database names before production runs, and coordinate credential rotation with the infrastructure team because migrations may need prolonged connections.
