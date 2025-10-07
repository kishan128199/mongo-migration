const { ObjectId } = require("mongodb");

module.exports = {
  description:
    "Copy feedback_questions.consolidated_report payloads into consolidated_reports and link back references.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const sourceQuestions = sourceDb.collection("feedback_questions");
    const targetQuestions = targetDb.collection("feedback_questions");
    const consolidatedCollection = targetDb.collection("consolidated_reports");

    logger.info(
      "Starting feedback_questions consolidated report linking migration"
    );

    const cursor = sourceQuestions
      .find({ consolidated_report: { $exists: true, $ne: null } })
      .batchSize(batchSize);

    let processed = 0;
    let consolidatedUpserts = 0;
    let questionLinks = 0;
    let skipped = 0;

    for await (const question of cursor) {
      processed += 1;

      const payload = extractPayload(question.consolidated_report);
      if (!payload) {
        skipped += 1;
        continue;
      }

      const questionId = ensureObjectId(question._id);
      if (!questionId) {
        logger.warn(
          "Skipping feedback_question without a usable _id when linking consolidated report payload."
        );
        skipped += 1;
        continue;
      }

      const consolidatedId =
        ensureObjectId(payload._id) ||
        (questionId instanceof ObjectId ? questionId : new ObjectId());

      const consolidatedDoc = buildConsolidatedDoc({
        payload,
        questionId,
        consolidatedId,
      });

      if (dryRun) {
        logger.info(
          `Dry run - would upsert consolidated_reports document ${describeId(
            consolidatedId
          )} for feedback_question ${describeId(questionId)}`
        );
      } else {
        await consolidatedCollection.updateOne(
          { _id: consolidatedId },
          { $set: consolidatedDoc },
          { upsert: true }
        );
      }
      consolidatedUpserts += 1;

      if (dryRun) {
        logger.info(
          `Dry run - would set feedback_questions document ${describeId(
            questionId
          )} consolidatedReportOf = ${describeId(consolidatedId)}`
        );
        questionLinks += 1;
      } else {
        const result = await targetQuestions.updateOne(
          { _id: questionId },
          { $set: { consolidatedReportOf: consolidatedId } }
        );

        if (result.matchedCount === 0) {
          logger.warn(
            `No target feedback_questions document found for _id ${describeId(
              questionId
            )} when linking consolidated report ${describeId(consolidatedId)}`
          );
        } else {
          questionLinks += 1;
        }
      }

      if (processed % batchSize === 0) {
        logger.info(
          `Processed ${processed} feedback_questions so far (consolidated reports: ${consolidatedUpserts}, links: ${questionLinks}, skipped: ${skipped}).`
        );
      }
    }

    logger.info(
      `Finished processing ${processed} feedback_questions. Consolidated reports upserted: ${consolidatedUpserts}, questions linked: ${questionLinks}, skipped: ${skipped}.`
    );
  },
};

function buildConsolidatedDoc({ payload, questionId, consolidatedId }) {
  const createdAt = normalizeDate(payload.createdAt) || new Date();
  const updatedAt =
    normalizeDate(payload.updatedAt) ||
    normalizeDate(payload.createdAt) ||
    createdAt;

  const reportPayload =
    payload && typeof payload.report === "object" && payload.report !== null
      ? cloneValue(payload.report)
      : cloneValue(payload);

  const doc = {
    _id: consolidatedId,
    sourceType: "question",
    consolidatedReportOf: questionId,
    status: extractNumericStatus(payload.status),
    report: reportPayload,
    createdAt,
    updatedAt,
  };

  return removeUndefinedKeys(doc);
}

function extractPayload(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  return value;
}

function extractNumericStatus(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return undefined;
}

function normalizeDate(value) {
  if (!value) {
    return undefined;
  }

  if (value instanceof Date) {
    return value;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return undefined;
  }

  return date;
}

function ensureObjectId(value) {
  if (value instanceof ObjectId) {
    return value;
  }

  if (typeof value === "string" && ObjectId.isValid(value)) {
    return new ObjectId(value);
  }

  if (
    value &&
    typeof value === "object" &&
    value.$oid &&
    ObjectId.isValid(value.$oid)
  ) {
    return new ObjectId(value.$oid);
  }

  return null;
}

function removeUndefinedKeys(doc) {
  if (!doc || typeof doc !== "object") {
    return doc;
  }

  for (const key of Object.keys(doc)) {
    if (doc[key] === undefined) {
      delete doc[key];
      continue;
    }

    if (Array.isArray(doc[key])) {
      doc[key] = doc[key]
        .map(item =>
          typeof item === "object" ? removeUndefinedKeys(item) || item : item
        )
        .filter(item => item !== undefined);
      continue;
    }

    if (doc[key] && typeof doc[key] === "object") {
      removeUndefinedKeys(doc[key]);
    }
  }

  return doc;
}

function cloneValue(value) {
  if (Array.isArray(value)) {
    return value.map(cloneValue);
  }

  if (value instanceof Date) {
    return new Date(value);
  }

  if (value && typeof value === "object") {
    const cloned = {};
    for (const [key, val] of Object.entries(value)) {
      cloned[key] = cloneValue(val);
    }
    return cloned;
  }

  return value;
}

function describeId(value) {
  if (!value) {
    return "<unknown>";
  }

  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (value && typeof value === "object" && value.$oid) {
    return String(value.$oid);
  }

  return String(value);
}
