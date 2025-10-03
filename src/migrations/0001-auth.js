module.exports = {
  description: 'Copy auth collection documents from source to target without modification.',

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const source = sourceDb.collection('auth');
    const target = targetDb.collection('auth');

    logger.info('Starting auth collection copy');

    const cursor = source.find({}).batchSize(batchSize);
    let processed = 0;

    for await (const doc of cursor) {
      if (dryRun) {
        logger.info(`Dry run - would upsert _id ${doc._id}`);
      } else {
        await target.updateOne(
          { _id: doc._id },
          { $set: doc },
          { upsert: true }
        );
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(`Processed ${processed} documents`);
      }
    }

    logger.info(`Finished auth collection copy. Total processed: ${processed}`);
  },
};
