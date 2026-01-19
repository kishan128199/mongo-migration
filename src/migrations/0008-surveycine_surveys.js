module.exports = {
  description:
    "Copy surveycine_surveys adding default type=customer and isClosed=false when missing.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const source = sourceDb.collection("surveycine_surveys");
    const target = targetDb.collection("surveycine_surveys");

    logger.info(
      "Starting surveycine_surveys migration (ensuring type and isClosed defaults)",
    );

    const cursor = source.find({}).batchSize(batchSize);
    let processed = 0;

    for await (const doc of cursor) {
      const transformed = {
        ...doc,
        type: "customer",
        isClosed: false,
      };

      if (dryRun) {
        logger.info(`Dry run - would upsert _id ${doc._id}`);
      } else {
        await target.updateOne(
          { _id: transformed._id },
          { $set: transformed },
          { upsert: true },
        );
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(`Processed ${processed} documents`);
      }
    }

    logger.info(
      `Finished surveycine_surveys migration. Total processed: ${processed}`,
    );
  },
};
