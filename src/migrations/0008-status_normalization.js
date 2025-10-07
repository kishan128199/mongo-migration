const { MongoServerError } = require("mongodb");

const STATUS_KEY = "status";

const isStatusKey = (key) => typeof key === "string" && key.toLowerCase() === STATUS_KEY;

const shouldNormalizeStatusValue = (value) => value === 200 || value === null;

const toDotNotation = (segments) => segments.map((segment) => segment.toString()).join(".");

const collectStatusPaths = (node, path = []) => {
  if (Array.isArray(node)) {
    const paths = [];
    for (let index = 0; index < node.length; index += 1) {
      paths.push(...collectStatusPaths(node[index], path.concat(index)));
    }

    return paths;
  }

  if (!node || typeof node !== "object") {
    return [];
  }

  const paths = [];

  for (const [key, value] of Object.entries(node)) {
    const nextPath = path.concat(key);

    if (isStatusKey(key) && shouldNormalizeStatusValue(value)) {
      paths.push(toDotNotation(nextPath));
    }

    paths.push(...collectStatusPaths(value, nextPath));
  }

  return paths;
};

module.exports = {
  description:
    "Normalize status fields across ml-v3 collections by setting 200/null values to 2.",

  async up({ targetDb, dryRun, logger }) {
    if (!targetDb) {
      throw new Error("Target database handle is required for status normalization.");
    }

    const collections = await targetDb
      .listCollections({}, { nameOnly: true })
      .toArray();

    let collectionsProcessed = 0;
    let collectionsUpdated = 0;
    let documentsMatched = 0;
    let documentsModified = 0;
    let statusFieldsTargeted = 0;

    for (const { name } of collections) {
      if (!name || name.startsWith("system.")) {
        logger.debug?.(
          `Skipping collection ${name ?? "<unknown>"} during status normalization.`
        );
        continue;
      }

      const collection = targetDb.collection(name);
      const cursor = collection.find({}, { batchSize: 200 });

      let collectionMatchedDocuments = 0;
      let collectionStatusFields = 0;

      for await (const doc of cursor) {
        const statusPaths = Array.from(new Set(collectStatusPaths(doc)));

        if (statusPaths.length === 0) {
          continue;
        }

        collectionMatchedDocuments += 1;
        collectionStatusFields += statusPaths.length;
        documentsMatched += 1;
        statusFieldsTargeted += statusPaths.length;

        logger.debug?.(
          `Identified status field paths ${statusPaths.join(", ")} in ${name} for _id=${doc._id}.`
        );

        if (dryRun) {
          continue;
        }

        const update = {};
        for (const path of statusPaths) {
          update[path] = 2;
        }

        try {
          const result = await collection.updateOne({ _id: doc._id }, { $set: update });
          documentsModified += result.modifiedCount ?? 0;
        } catch (error) {
          if (error instanceof MongoServerError) {
            logger.error(
              `Failed to normalize status fields in ${name} for _id=${doc._id}: ${error.message}`
            );
          }

          throw error;
        }
      }

      collectionsProcessed += 1;

      if (collectionMatchedDocuments === 0) {
        continue;
      }

      collectionsUpdated += 1;

      if (dryRun) {
        logger.info(
          `Dry run - would normalize ${collectionMatchedDocuments} documents across ${collectionStatusFields} status field(s) in ${name}.`
        );
        continue;
      }

      logger.info(
        `Normalized ${collectionMatchedDocuments} documents across ${collectionStatusFields} status field(s) in ${name}.`
      );
    }

    const statusFieldLabel = dryRun
      ? "status fields identified"
      : "status fields normalized";

    logger.info(
      `Status normalization complete. Collections processed: ${collectionsProcessed}, collections updated: ${collectionsUpdated}, documents matched: ${documentsMatched}, documents modified: ${documentsModified}, ${statusFieldLabel}: ${statusFieldsTargeted}.`
    );
  },
};
