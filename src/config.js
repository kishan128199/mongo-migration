const path = require('path');
const dotenv = require('dotenv');

dotenv.config({ path: process.env.ENV_PATH || path.resolve(process.cwd(), '.env') });

const DEFAULT_BATCH_SIZE = 500;

function loadConfig(overrides = {}, options = {}) {
  const { validate = true } = options;

  const config = {
    sourceUri: process.env.SOURCE_MONGO_URI || process.env.MONGO_URI || '',
    targetUri: process.env.TARGET_MONGO_URI || process.env.MONGO_URI || '',
    sourceDb: process.env.SOURCE_DB_NAME || '',
    targetDb: process.env.TARGET_DB_NAME || process.env.SOURCE_DB_NAME || '',
    batchSize: Number(process.env.BATCH_SIZE || DEFAULT_BATCH_SIZE),
    dryRun: process.env.DRY_RUN === 'true' || process.env.DRY_RUN === '1',
    migrationsDir: process.env.MIGRATIONS_DIR || path.join(process.cwd(), 'src', 'migrations'),
    ...overrides,
  };

  if (validate) {
    const missing = ['sourceUri', 'targetUri', 'sourceDb', 'targetDb'].filter((key) => !config[key]);
    if (missing.length) {
      throw new Error(`Missing required configuration values: ${missing.join(', ')}`);
    }

    if (!Number.isFinite(config.batchSize) || config.batchSize <= 0) {
      throw new Error('BATCH_SIZE must be a positive number');
    }
  }

  return config;
}

module.exports = {
  loadConfig,
  DEFAULT_BATCH_SIZE,
};
