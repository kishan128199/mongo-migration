const { ObjectId } = require("mongodb");

module.exports = {
  description:
    "Populate surveycine_surveys and consolidated_reports collections from ml-v2 feedback_requests.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const sourceCollection = sourceDb.collection("feedback_requests");
    const surveycineCollection = targetDb.collection("surveycine_surveys");
    const consolidatedCollection = targetDb.collection("consolidated_reports");

    logger.info(
      "Starting migration: feedback_requests -> surveycine_surveys & consolidated_reports"
    );

    const cursor = sourceCollection.find({}).batchSize(batchSize);

    let processed = 0;
    let surveycineUpserts = 0;
    let consolidatedUpserts = 0;
    let missingConsolidated = 0;

    for await (const feedbackRequest of cursor) {
      const surveycineDoc = buildSurveycineDocument(feedbackRequest);
      const consolidatedDoc = buildConsolidatedReportDocument(feedbackRequest);

      if (dryRun) {
        logger.info(
          `Dry run - would upsert surveycine_surveys document ${describeId(
            surveycineDoc._id
          )}`
        );
      } else {
        await surveycineCollection.replaceOne(
          { _id: surveycineDoc._id },
          surveycineDoc,
          { upsert: true }
        );
      }

      surveycineUpserts += 1;

      if (consolidatedDoc) {
        if (dryRun) {
          logger.info(
            `Dry run - would upsert consolidated_reports document ${describeId(
              consolidatedDoc._id
            )} for feedback ${describeId(surveycineDoc._id)}`
          );
        } else {
          await consolidatedCollection.replaceOne(
            { _id: consolidatedDoc._id },
            consolidatedDoc,
            { upsert: true }
          );
        }

        consolidatedUpserts += 1;
      } else {
        missingConsolidated += 1;
        logger.warn(
          `No consolidated_reports payload found for feedback ${describeId(
            surveycineDoc._id
          )}; skipping consolidated_reports upsert.`
        );
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(
          `Processed ${processed} feedback_requests (surveycine: ${surveycineUpserts}, consolidated: ${consolidatedUpserts})`
        );
      }
    }

    logger.info(
      `Migration complete. Total feedback_requests processed: ${processed}. surveycine_surveys upserts: ${surveycineUpserts}, consolidated_reports upserts: ${consolidatedUpserts}, missing consolidated payloads: ${missingConsolidated}.`
    );
  },
};

function buildSurveycineDocument(feedbackRequest) {
  const cloned = cloneValue(feedbackRequest);
  cloned._id = ensureObjectId(feedbackRequest._id);

  if ("consolidated_reports" in cloned) {
    delete cloned.consolidated_reports;
  }

  if ("consolidatedReport" in cloned) {
    delete cloned.consolidatedReport;
  }

  removeUndefinedKeys(cloned);
  return cloned;
}

function buildConsolidatedReportDocument(feedbackRequest) {
  const source =
    feedbackRequest && typeof feedbackRequest === "object"
      ? feedbackRequest.consolidated_reports
      : null;

  if (!source || typeof source !== "object") {
    return null;
  }

  const consolidatedRef = ensureObjectId(feedbackRequest._id);
  const consolidatedId = ensureObjectId(source._id) || consolidatedRef;

  const createdAt =
    normalizeDate(source.createdAt) ||
    normalizeDate(feedbackRequest.createdAt) ||
    new Date();
  const updatedAt =
    normalizeDate(source.updatedAt) ||
    normalizeDate(feedbackRequest.updatedAt) ||
    createdAt;

  const reportPayload =
    source.report && typeof source.report === "object" ? source.report : source;

  const reportDoc = {
    _id: consolidatedId,
    sourceType: "surveycine",
    consolidatedReportOf: consolidatedRef,
    status: determineStatus(source, feedbackRequest),
    report: buildReport(reportPayload),
    createdAt,
    updatedAt,
  };

  removeUndefinedKeys(reportDoc);
  return reportDoc;
}

function determineStatus(source, feedbackRequest) {
  if (source && source.status !== undefined) {
    return source.status;
  }

  if (source && source.report && source.report.status !== undefined) {
    return source.report.status;
  }

  if (feedbackRequest && feedbackRequest.status !== undefined) {
    return feedbackRequest.status;
  }

  return 0;
}

function buildReport(source) {
  const sentiment = extractNestedValue(source, "sentiment");
  const trueValueScore = extractNestedValue(
    source,
    "trueValueScore",
    "true_value_score"
  );
  const opinionSnippets = extractNestedValue(
    source,
    "opinionSnippets",
    "opinion_snippets"
  );
  const bias = extractNestedValue(source, "bias");
  const brandRecallScore = extractNestedValue(
    source,
    "brandRecallScore",
    "brand_recall_score"
  );

  const report = {
    sentiment: cloneObjectOrEmpty(sentiment),
    trueValueScore: cloneObjectOrEmpty(trueValueScore),
    opinionSnippets: normalizeOpinionSnippets(opinionSnippets),
    verdict: extractNestedValue(source, "verdict") ?? "",
    bias: cloneObjectOrEmpty(bias),
    brandRecallScore: cloneObjectOrEmpty(brandRecallScore),
  };

  removeUndefinedKeys(report);
  return report;
}

function normalizeOpinionSnippets(value) {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter(snippet => snippet && typeof snippet === "object")
    .map(snippet => {
      const normalized = {
        text: snippet.text,
        time: snippet.time,
        thumbnail: snippet.thumbnail,
      };

      removeUndefinedKeys(normalized);
      return normalized;
    });
}

function extractNestedValue(source, ...keys) {
  if (!source || typeof source !== "object") {
    return undefined;
  }

  for (const key of keys) {
    if (source[key] !== undefined) {
      return source[key];
    }
  }

  return undefined;
}

function cloneObjectOrEmpty(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  return cloneValue(value);
}

function cloneValue(value) {
  if (Array.isArray(value)) {
    return value.map(cloneValue);
  }

  if (isPlainObject(value)) {
    const result = {};
    for (const [key, val] of Object.entries(value)) {
      result[key] = cloneValue(val);
    }
    return result;
  }

  return value;
}

function isPlainObject(value) {
  if (!value || typeof value !== "object") {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function ensureObjectId(value) {
  if (!value) {
    return value;
  }

  if (value instanceof ObjectId) {
    return value;
  }

  if (typeof value === "string" && ObjectId.isValid(value)) {
    return new ObjectId(value);
  }

  if (typeof value === "object" && value.$oid && ObjectId.isValid(value.$oid)) {
    return new ObjectId(value.$oid);
  }

  return value;
}

function normalizeDate(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return value;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed;
}

function describeId(value) {
  if (!value) {
    return "<unknown>";
  }

  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (typeof value === "object" && value.$oid) {
    return String(value.$oid);
  }

  return String(value);
}

function removeUndefinedKeys(doc) {
  if (!doc || typeof doc !== "object") {
    return;
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
