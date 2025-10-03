#!/usr/bin/env node

const yargs = require('yargs/yargs');
const { loadConfig } = require('./config');
const { createLogger } = require('./lib/logger');
const { listMigrations, loadMigration } = require('./lib/migrationLoader');
const { runMigration } = require('./lib/migrationRunner');

async function listHandler(argv) {
  const overrides = {};
  if (argv.migrationsDir) {
    overrides.migrationsDir = argv.migrationsDir;
  }

  const config = loadConfig(overrides, { validate: false });

  const migrations = await listMigrations(config.migrationsDir);
  if (!migrations.length) {
    console.log('No migration files found.');
    return;
  }

  migrations.forEach((migration) => {
    console.log(`- ${migration.id}`);
  });
}

async function runHandler(argv) {
  const overrides = {};

  if (argv.migrationsDir) overrides.migrationsDir = argv.migrationsDir;
  if (argv.sourceUri) overrides.sourceUri = argv.sourceUri;
  if (argv.targetUri) overrides.targetUri = argv.targetUri;
  if (argv.sourceDb) overrides.sourceDb = argv.sourceDb;
  if (argv.targetDb) overrides.targetDb = argv.targetDb;
  if (Number.isFinite(argv.batchSize)) overrides.batchSize = argv.batchSize;

  const config = loadConfig(overrides);

  if (typeof argv.dryRun === 'boolean') {
    config.dryRun = argv.dryRun;
  }

  const logger = createLogger('cli');
  const migration = await loadMigration(config.migrationsDir, argv.name);

  await runMigration(migration, config, logger);
}

function parse() {
  return yargs(process.argv.slice(2))
    .scriptName('mongo-migrate')
    .command(
      'list',
      'List available migration scripts',
      (cmd) =>
        cmd.option('migrations-dir', {
          type: 'string',
          describe: 'Override the migrations directory relative to the project root',
        }),
      (argv) => execute(listHandler(argv))
    )
    .command(
      ['run <name>', '$0 <name>'],
      'Run a specific migration by file name without the .js extension',
      (cmd) =>
        cmd
          .positional('name', {
            type: 'string',
            describe: 'Migration identifier (file name without extension)',
          })
          .option('source-uri', {
            type: 'string',
            describe: 'MongoDB connection string for the source database',
          })
          .option('target-uri', {
            type: 'string',
            describe: 'MongoDB connection string for the target database',
          })
          .option('source-db', {
            type: 'string',
            describe: 'Name of the source database',
          })
          .option('target-db', {
            type: 'string',
            describe: 'Name of the target database',
          })
          .option('batch-size', {
            type: 'number',
            describe: 'Number of documents to process per batch',
          })
          .option('dry-run', {
            type: 'boolean',
            describe: 'Simulate the migration without writing to the target database',
          })
          .option('migrations-dir', {
            type: 'string',
            describe: 'Override the migrations directory relative to the project root',
          }),
      (argv) => execute(runHandler(argv))
    )
    .demandCommand(1, 'Specify a command, e.g. `mongo-migrate list`.')
    .strict()
    .help();
}

async function execute(promise) {
  try {
    await promise;
  } catch (error) {
    console.error(`\nMigration failed: ${error.message}`);
    if (process.env.DEBUG) {
      console.error(error.stack);
    }
    process.exitCode = 1;
  }
}

parse().parse();
