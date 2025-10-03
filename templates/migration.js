module.exports = {
  description: 'Describe what this migration changes.',

  /**
   * @param {Object} context
   * @param {import('mongodb').Db} context.sourceDb
   * @param {import('mongodb').Db} context.targetDb
   * @param {number} context.batchSize
   * @param {boolean} context.dryRun
   * @param {{ info: Function, warn: Function, error: Function }} context.logger
   * @param {Object} context.config
   */
  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    logger.info('Running sample migration template');

    const sourceCollection = sourceDb.collection('source_collection');
    const targetCollection = targetDb.collection('target_collection');

    const cursor = sourceCollection.find({}).batchSize(batchSize);

    for await (const doc of cursor) {
      const transformed = transformDocument(doc);

      if (dryRun) {
        logger.info(`Dry run - would upsert document with _id ${transformed._id}`);
        continue;
      }

      await targetCollection.updateOne(
        { _id: transformed._id },
        { $set: transformed },
        { upsert: true }
      );
    }

    logger.info('Template migration completed');
  },
};

function transformDocument(doc) {
  return {
    ...doc,
    migratedAt: new Date(),
  };
}
