module.exports = {
  description:
    "Copy unit_usage collection documents while dropping projectId and collectionRef.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const source = sourceDb.collection("unit_usage");
    const target = targetDb.collection("unit_usage");

    logger.info("Starting unit_usage migration (removing collectionRef)");

    const cursor = source.find({}).batchSize(batchSize);
    let processed = 0;
    const removedFieldCounts = {
      collectionRef: 0,
    };

    for await (const doc of cursor) {
      const transformed = { ...doc };
      const removedFields = [];

      for (const field of Object.keys(removedFieldCounts)) {
        if (Object.prototype.hasOwnProperty.call(transformed, field)) {
          delete transformed[field];
          removedFields.push(field);
          removedFieldCounts[field] += 1;
        }
      }

      if (dryRun) {
        const suffix = removedFields.length
          ? ` (removing ${removedFields.join(", ")})`
          : "";
        logger.info(`Dry run - would upsert _id ${doc._id}${suffix}`);
      } else {
        await target.updateOne(
          { _id: transformed._id },
          { $set: transformed },
          { upsert: true }
        );
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(`Processed ${processed} documents so far`);
      }
    }

    logger.info(
      `Finished unit_usage migration. Processed ${processed} documents. Removed collectionRef from ${removedFieldCounts.collectionRef} documents.`
    );
  },
};
