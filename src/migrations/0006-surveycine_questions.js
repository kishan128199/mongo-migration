module.exports = {
  description: 'Copy surveycine_questions collection, removing consolidated_report field during migration.',

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const source = sourceDb.collection('surveycine_questions');
    const target = targetDb.collection('surveycine_questions');

    logger.info('Starting surveycine_questions migration (dropping consolidated_report)');

    const cursor = source.find({}).batchSize(batchSize);
    let processed = 0;

    for await (const doc of cursor) {
      const { _id, consolidated_report, ...rest } = doc;
      const update = { $set: rest };

      if (Object.prototype.hasOwnProperty.call(doc, 'consolidated_report')) {
        update.$unset = { consolidated_report: '' };
      }

      if (dryRun) {
        logger.info(`Dry run - would upsert _id ${_id}`);
      } else {
        await target.updateOne({ _id }, update, { upsert: true });
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(`Processed ${processed} documents`);
      }
    }

    logger.info(`Finished surveycine_questions migration. Total processed: ${processed}`);
  },
};
