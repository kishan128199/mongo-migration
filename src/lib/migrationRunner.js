const { openConnections, closeConnections } = require('./mongo');

async function runMigration(migration, config, logger) {
  const log = logger.child(`migration:${migration.id}`);
  const connections = await openConnections(config, logger);
  const context = {
    sourceDb: connections.sourceDb,
    targetDb: connections.targetDb,
    batchSize: config.batchSize,
    dryRun: config.dryRun,
    logger: log,
    config,
  };

  if (config.dryRun) {
    log.warn('Running in dry-run mode. No target writes will be persisted.');
  }

  try {
    log.info('Starting migration');
    await migration.up(context);
    log.info('Migration finished successfully');
  } finally {
    await closeConnections(connections);
  }
}

module.exports = {
  runMigration,
};
