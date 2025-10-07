const { ObjectId } = require("mongodb");
const crypto = require("crypto");

module.exports = {
  description:
    "Populate surveycine_responses, insights, and reports collections from ml-v2 feedback_responses.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const sourceCollection = sourceDb.collection("feedback_responses");
    const surveycineResponses = targetDb.collection("surveycine_responses");
    const insightsCollection = targetDb.collection("insights");
    const reportsCollection = targetDb.collection("reports");

    const cursor = sourceCollection.find({}).batchSize(batchSize);

    let processed = 0;
    let surveycineUpserts = 0;
    let insightUpserts = 0;
    let reportUpserts = 0;
    let skippedSurveycine = 0;
    let skippedInsights = 0;
    let skippedReports = 0;
    let withoutRecordId = 0;

    for await (const feedbackResponse of cursor) {
      const surveycineDoc = buildSurveycineResponseDocument(feedbackResponse);

      if (!surveycineDoc || !surveycineDoc._id) {
        skippedSurveycine += 1;
        logger.warn(
          `Skipping feedback_responses document ${describeId(
            feedbackResponse._id
          )} - unable to determine surveycine_responses _id.`
        );
        continue;
      }

      if (dryRun) {
        logger.info(
          `Dry run - would upsert surveycine_responses document ${describeId(
            surveycineDoc._id
          )}`
        );
      } else {
        await surveycineResponses.replaceOne(
          { _id: surveycineDoc._id },
          surveycineDoc,
          { upsert: true }
        );
      }

      surveycineUpserts += 1;

      const hasRecordId = Boolean(feedbackResponse.recordId);
      if (!hasRecordId) {
        withoutRecordId += 1;
      }

      if (hasRecordId) {
        const insightDoc = buildSurveycineInsightDocument(
          feedbackResponse,
          surveycineDoc._id
        );

        if (insightDoc) {
          if (dryRun) {
            logger.info(
              `Dry run - would upsert insight ${describeId(
                insightDoc._id
              )} for surveycine response ${describeId(surveycineDoc._id)}`
            );
          } else {
            await insightsCollection.replaceOne(
              { _id: insightDoc._id },
              insightDoc,
              { upsert: true }
            );
          }

          insightUpserts += 1;
        } else {
          skippedInsights += 1;
        }

        const reportDoc = buildSurveycineReportDocument(feedbackResponse);
        if (reportDoc) {
          if (dryRun) {
            logger.info(
              `Dry run - would upsert report ${describeId(
                reportDoc._id
              )} for feedback response ${describeId(feedbackResponse._id)}`
            );
          } else {
            await reportsCollection.replaceOne(
              { _id: reportDoc._id },
              reportDoc,
              { upsert: true }
            );
          }

          reportUpserts += 1;
        } else {
          skippedReports += 1;
        }
      }

      processed += 1;
      if (processed % batchSize === 0) {
        logger.info(
          `Processed ${processed} feedback_responses (surveycine: ${surveycineUpserts}, insights: ${insightUpserts}, reports: ${reportUpserts})`
        );
      }
    }

    logger.info(
      `Migration finished. Processed: ${processed}, surveycine_responses upserts: ${surveycineUpserts}, insights upserts: ${insightUpserts}, reports upserts: ${reportUpserts}, without recordId: ${withoutRecordId}, skipped surveycine docs: ${skippedSurveycine}, skipped insights: ${skippedInsights}, skipped reports: ${skippedReports}.`
    );
  },
};

function buildSurveycineResponseDocument(feedbackResponse) {
  const responseId = toObjectId(feedbackResponse._id) ?? feedbackResponse._id;

  const doc = {
    _id: responseId,
    name: feedbackResponse.name || feedbackResponse.userName,
    city: feedbackResponse.city,
    age: ensureNumber(feedbackResponse.age),
    gender: feedbackResponse.gender,
    feedbackId: toObjectId(
      feedbackResponse.feedbackId || feedbackResponse.feedback_id
    ),
    isDeleted: coerceBoolean(
      feedbackResponse.isDeleted !== undefined
        ? feedbackResponse.isDeleted
        : feedbackResponse.deleted
    ),
    startProcessing: coerceBoolean(feedbackResponse.startProcessing),
    allResponsesSubmitted: coerceBoolean(
      feedbackResponse.allResponsesSubmitted
    ),
    createdAt: normalizeDate(feedbackResponse.createdAt),
    updatedAt: normalizeDate(feedbackResponse.updatedAt),
  };

  if (
    feedbackResponse.recordId !== undefined &&
    feedbackResponse.recordId !== null
  ) {
    doc.recordId =
      toObjectId(feedbackResponse.recordId) ?? feedbackResponse.recordId;
  }

  removeUndefinedKeys(doc);
  return doc;
}

function buildSurveycineInsightDocument(feedbackResponse, surveycineId) {
  const insightId = deriveDeterministicObjectId(feedbackResponse._id, 1);
  const speechToText = normalizeSpeechToText(feedbackResponse);
  const video = normalizeVideoSection(feedbackResponse);
  const audio = normalizeAudioSection(feedbackResponse);
  const trueValueScore = normalizeTrueValueScore(feedbackResponse);
  const thumbnail = normalizeThumbnail(feedbackResponse);
  const questions =
    Array.isArray(feedbackResponse.questions) &&
    feedbackResponse.questions.length > 0
      ? feedbackResponse.questions
      : undefined;

  const insight = {
    _id: insightId,
    insightsOf:
      surveycineId instanceof ObjectId
        ? surveycineId
        : toObjectId(surveycineId) ?? surveycineId,
    insightsOfType: "surveycine",
    video: isEmptyValue(video) ? undefined : video,
    audio: isEmptyValue(audio) ? undefined : audio,
    speechToText: isEmptyValue(speechToText) ? undefined : speechToText,
    trueValueScore: isEmptyValue(trueValueScore) ? undefined : trueValueScore,
    thumbnail: isEmptyValue(thumbnail) ? undefined : thumbnail,
    questions,
    embeddingsGenerated: Boolean(feedbackResponse.isSaveEmbeddings),
    message: feedbackResponse.message,
    createdAt: normalizeDate(feedbackResponse.createdAt),
    updatedAt: normalizeDate(feedbackResponse.updatedAt),
  };

  const rawScore =
    feedbackResponse.true_value_score_raw !== undefined
      ? feedbackResponse.true_value_score_raw
      : feedbackResponse.trueValueScoreRaw;

  if (rawScore !== undefined) {
    const rawEntries = normalizeTrueValueScoreRaw(rawScore);
    if (rawEntries.length > 0) {
      if (!insight.trueValueScore) {
        insight.trueValueScore = {};
      }
      insight.trueValueScore.raw = rawEntries;
    }
  }

  const hasPayload =
    !isEmptyValue(insight.video) ||
    !isEmptyValue(insight.audio) ||
    !isEmptyValue(insight.speechToText) ||
    !isEmptyValue(insight.trueValueScore) ||
    !isEmptyValue(insight.thumbnail) ||
    (Array.isArray(insight.questions) && insight.questions.length > 0) ||
    insight.embeddingsGenerated ||
    insight.message !== undefined;

  if (!hasPayload) {
    return null;
  }

  removeUndefinedKeys(insight);
  return insight;
}

function buildSurveycineReportDocument(feedbackResponse) {
  const report = feedbackResponse.report;
  if (!report || typeof report !== "object") {
    return null;
  }

  const normalizedReport = cloneValue(report);
  const createdAt =
    normalizeDate(report.createdAt) ||
    normalizeDate(feedbackResponse.createdAt);
  const updatedAt =
    normalizeDate(report.updatedAt) ||
    normalizeDate(feedbackResponse.updatedAt) ||
    createdAt;

  const doc = {
    _id: deriveDeterministicObjectId(feedbackResponse._id, 2),
    reportOfId: toObjectId(feedbackResponse._id) || feedbackResponse._id,
    reportOfType: "surveycine",
    report: normalizedReport,
    status:
      report.status !== undefined ? report.status : feedbackResponse.status,
    message:
      report.message !== undefined ? report.message : feedbackResponse.message,
    createdAt,
    updatedAt,
  };

  removeUndefinedKeys(doc);
  return doc;
}

function normalizeVideoSection(response) {
  const video = isPlainObject(response.video) ? response.video : {};
  const duration =
    video.duration ??
    response.speech_to_text?.duration ??
    response.speechToText?.duration ??
    undefined;

  const data = pruneUndefined({
    bucketUrl:
      response.video_url ||
      response.videoUrl ||
      video.bucketUrl ||
      video.url ||
      response.responseFile,
    file: video.file || response.responseFile,
    status: video.status,
    message: video.message,
    duration,
  });

  if (isEmptyValue(data)) {
    return {};
  }

  const section = {};

  if (data.status !== undefined) {
    section.status = data.status;
    delete data.status;
  }

  if (data.message !== undefined) {
    section.message = data.message;
    delete data.message;
  }

  if (!isEmptyValue(data)) {
    section.data = data;
  }

  return section;
}

function normalizeAudioSection(response) {
  const audio = isPlainObject(response.audio) ? response.audio : {};

  const data = pruneUndefined({
    bucketUrl: response.audio_url || audio.bucketUrl || audio.url,
    file: audio.file,
  });

  if (audio.status !== undefined) {
    data.status = audio.status;
  }

  if (audio.message !== undefined) {
    data.message = audio.message;
  }

  if (isEmptyValue(data)) {
    return {};
  }

  const section = {};

  if (data.status !== undefined) {
    section.status = data.status;
    delete data.status;
  }

  if (data.message !== undefined) {
    section.message = data.message;
    delete data.message;
  }

  if (!isEmptyValue(data)) {
    section.data = data;
  }

  return section;
}

function normalizeSpeechToText(response) {
  const speech = isPlainObject(response.speech_to_text)
    ? response.speech_to_text
    : isPlainObject(response.speechToText)
    ? response.speechToText
    : null;

  if (!speech) {
    return {};
  }

  const normalized = pruneUndefined({
    status: speech.status,
    data: speech.data,
    duration: speech.duration,
    message: speech.message,
  });

  return normalized;
}

function normalizeTrueValueScore(response) {
  const source =
    response.true_value_score !== undefined
      ? response.true_value_score
      : response.trueValueScore;

  if (source === undefined || source === null) {
    return {};
  }

  const result = {};

  if (isPlainObject(source)) {
    if (source.status !== undefined) {
      result.status = source.status;
    }

    if (source.message !== undefined) {
      result.message = source.message;
    }

    if (source.data !== undefined) {
      result.data = cloneValue(source.data);
    } else {
      const clone = { ...source };
      delete clone.status;
      delete clone.message;
      if (!isEmptyValue(clone)) {
        result.data = clone;
      }
    }
  } else {
    result.data = source;
  }

  removeUndefinedKeys(result);
  return result;
}

function normalizeTrueValueScoreRaw(raw) {
  if (raw === undefined || raw === null) {
    return [];
  }

  if (Array.isArray(raw)) {
    return raw.filter(item => item !== undefined && item !== null);
  }

  return [raw];
}

function normalizeThumbnail(response) {
  const thumbnail = isPlainObject(response.thumbnail)
    ? response.thumbnail
    : null;
  if (!thumbnail) {
    return {};
  }

  const section = pruneUndefined({
    status: thumbnail.status,
    message: thumbnail.message,
    data: pruneUndefined({
      ...thumbnail.files,
      processedAt: normalizeDate(thumbnail.processedAt),
    }),
  });

  if (section.data && isEmptyValue(section.data)) {
    delete section.data;
  }

  return section;
}

function normalizeDate(value) {
  if (!value) {
    return undefined;
  }

  if (value instanceof Date) {
    return value;
  }

  if (typeof value === "object" && value.$date) {
    const parsed = new Date(value.$date);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }

  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed;
  }

  return undefined;
}

function ensureNumber(value) {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function coerceBoolean(value) {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["true", "1", "yes"].includes(normalized)) {
      return true;
    }
    if (["false", "0", "no"].includes(normalized)) {
      return false;
    }
  }

  return undefined;
}

function pruneUndefined(obj) {
  if (!obj) {
    return {};
  }

  const result = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function removeUndefinedKeys(obj) {
  if (!obj || typeof obj !== "object") {
    return;
  }

  for (const key of Object.keys(obj)) {
    if (obj[key] === undefined) {
      delete obj[key];
    }
  }
}

function isEmptyValue(value) {
  if (value === undefined || value === null) {
    return true;
  }

  if (Array.isArray(value)) {
    return value.length === 0;
  }

  if (typeof value === "object") {
    return Object.keys(value).length === 0;
  }

  return false;
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function cloneValue(value) {
  if (value === null || value === undefined) {
    return value;
  }

  if (value instanceof Date) {
    return new Date(value);
  }

  if (Array.isArray(value)) {
    return value.map(cloneValue);
  }

  if (typeof value === "object") {
    const clone = {};
    for (const [key, val] of Object.entries(value)) {
      clone[key] = cloneValue(val);
    }
    return clone;
  }

  return value;
}

function deriveDeterministicObjectId(baseId, offset) {
  const objectId = toObjectId(baseId);
  if (objectId instanceof ObjectId) {
    const bytes = Buffer.from(objectId.id);
    bytes[bytes.length - 1] = (bytes[bytes.length - 1] + offset) & 0xff;
    return new ObjectId(bytes);
  }

  const digest = crypto
    .createHash("sha1")
    .update(String(baseId))
    .update(String(offset))
    .digest();

  return new ObjectId(digest.subarray(0, 12));
}

function toObjectId(value) {
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

function describeId(value) {
  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (value && typeof value === "object" && value.$oid) {
    return value.$oid;
  }

  return String(value);
}
