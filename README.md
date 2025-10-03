# MongoDB Schema Migration Scaffold

This project provides a Node.js CLI scaffold for migrating MongoDB collection schemas one migration at a time. It connects to both legacy and new MongoDB databases, runs scripted transformations, and supports dry-run execution while you validate results.

## Prerequisites

- Node.js 18+
- Access to the MongoDB instances you want to migrate between

## Getting started

1. Install dependencies:
   ```sh
   npm install
   ```
2. Copy the environment example and fill in your connection details:
   ```sh
   cp .env.example .env
   ```
3. Verify available migrations:
   ```sh
   npm run migrate -- list
   ```

## Running a migration

Execute migrations by referencing the file name (without the `.js` extension):

```sh
npm run migrate -- run 0001-sample-user-profile
```

Useful flags:
- `--dry-run` to simulate the migration without writing to the target database.
- `--batch-size <number>` to override how many documents are processed per iteration.
- `--source-uri`, `--target-uri`, `--source-db`, `--target-db` to override connection settings without editing the `.env` file.
- `--migrations-dir` to use a different folder for migration scripts.

## Creating your own migrations

1. Copy the template into the migrations directory with a descriptive, ordered name:
   ```sh
   cp templates/migration.js src/migrations/0002-new-schema.js
   ```
2. Update the `description` and the logic inside the `up` function. The runner injects:
   - `sourceDb` – MongoDB database instance for your legacy data
   - `targetDb` – MongoDB database instance for the new schema
   - `batchSize` – Configurable batch size (default `500`)
   - `dryRun` – Boolean flag indicating whether writes should be skipped
   - `logger` – Basic logger with `info`, `warn`, and `error`
3. Implement your transformation logic and write to the target collection. Keep migrations idempotent so they can be re-run safely.

The included `0001-sample-user-profile.js` demonstrates copying documents into a new schema while reshaping each document.

## Configuration reference

| Variable | Description |
| --- | --- |
| `SOURCE_MONGO_URI` | Connection string for the legacy MongoDB instance |
| `TARGET_MONGO_URI` | Connection string for the new MongoDB instance |
| `SOURCE_DB_NAME` | Database name containing the legacy collections |
| `TARGET_DB_NAME` | Database name for the new schema |
| `BATCH_SIZE` | Documents processed per batch (default `500`) |
| `DRY_RUN` | Set `true` or `1` to globally run migrations without writes |
| `MIGRATIONS_DIR` | Optional path override for migration scripts |

Set `DEBUG=true` when running the CLI to print full stack traces if errors occur.

## Next steps

- Replace the sample migration with your real collection mappings.
- Add tests or scripts to validate transformed documents before writing them.
- Wire the CLI into your deployment or CI workflow once migrations are ready.
