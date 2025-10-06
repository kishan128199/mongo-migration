const { ObjectId } = require("mongodb");

module.exports = {
  description:
    "Copy folder_reports.consolidated_report payloads into consolidated_reports and link back references.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const sourceFolderReports = sourceDb.collection("folder_reports");
    const targetFolderReports = targetDb.collection("folder_reports");
    const consolidatedCollection = targetDb.collection("consolidated_reports");

    logger.info(
      "Starting folder_reports consolidated report linking migration"
    );

    const cursor = sourceFolderReports
      .find({ consolidated_report: { $exists: true, $ne: null } })
      .batchSize(batchSize);

    let processed = 0;
    let skipped = 0;
    let consolidatedUpserts = 0;
    let folderLinks = 0;

    for await (const folderReport of cursor) {
      processed += 1;

      const payload = normalizePayload(folderReport.consolidated_report);
      if (!payload) {
        skipped += 1;
        continue;
      }

      const folderReportId = ensureObjectId(folderReport.folderId);
      if (!folderReportId) {
        logger.warn(
          `Skipping folder_report without usable _id when building consolidated report payload.`
        );
        skipped += 1;
        continue;
      }

      const consolidatedId =
        ensureObjectId(payload._id) ||
        (folderReportId instanceof ObjectId ? folderReportId : new ObjectId());

      const consolidatedDoc = buildConsolidatedDoc({
        payload,
        folderReport,
        folderReportId,
        consolidatedId,
      });

      if (dryRun) {
        logger.info(
          `Dry run - would upsert consolidated_reports document ${describeId(
            consolidatedId
          )} for folder_report ${describeId(folderReportId)}`
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
          `Dry run - would set folder_reports document ${describeId(
            folderReportId
          )} ConsolidatedReportOf = ${describeId(consolidatedId)}`
        );
        folderLinks += 1;
      } else {
        const result = await targetFolderReports.updateOne(
          { _id: folderReportId },
          { $set: { ConsolidatedReportOf: consolidatedId } }
        );

        if (result.matchedCount === 0) {
          logger.warn(
            `No target folder_reports document found for _id ${describeId(
              folderReportId
            )} when linking consolidated report ${describeId(consolidatedId)}`
          );
        } else {
          folderLinks += 1;
        }
      }

      if (processed % batchSize === 0) {
        logger.info(
          `Processed ${processed} folder_reports so far (consolidated reports: ${consolidatedUpserts}, links: ${folderLinks}, skipped: ${skipped}).`
        );
      }
    }

    logger.info(
      `Finished processing ${processed} folder_reports. Consolidated reports upserted: ${consolidatedUpserts}, folder documents linked: ${folderLinks}, skipped: ${skipped}.`
    );
  },
};

function buildConsolidatedDoc({
  payload,
  folderReport,
  folderReportId,
  consolidatedId,
}) {
  const createdAt =
    normalizeDate(payload.createdAt) ||
    normalizeDate(folderReport.createdAt) ||
    new Date();

  const updatedAt =
    normalizeDate(payload.updatedAt) ||
    normalizeDate(folderReport.updatedAt) ||
    createdAt;

  const reportPayload =
    payload && typeof payload.report === "object" && payload.report !== null
      ? payload.report
      : payload;

  const doc = {
    _id: consolidatedId,
    sourceType: "folder",
    ConsolidatedReportOf: folderReportId,
    status: determineStatus(payload, folderReport),
    report: cloneValue(reportPayload),
    consolidated_report: cloneValue(payload),
    createdAt,
    updatedAt,
  };

  removeUndefinedKeys(doc);
  return doc;
}

function normalizePayload(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value;
}

function determineStatus(payload, folderReport) {
  const fromPayload = extractNumericStatus(payload && payload.status);
  if (fromPayload !== null) {
    return fromPayload;
  }

  const fromReport = extractNumericStatus(
    payload && payload.report && payload.report.status
  );
  if (fromReport !== null) {
    return fromReport;
  }

  const fromFolder = extractNumericStatus(folderReport && folderReport.status);
  if (fromFolder !== null) {
    return fromFolder;
  }

  return 0;
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

  return null;
}

function normalizeDate(value) {
  if (!value) {
    return null;
  }

  if (value instanceof Date) {
    return value;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
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

  return value === undefined ? null : value;
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

  if (isPlainObject(value)) {
    const cloned = {};
    for (const [key, val] of Object.entries(value)) {
      cloned[key] = cloneValue(val);
    }
    return cloned;
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
