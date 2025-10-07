const { ObjectId } = require("mongodb");
const crypto = require("crypto");

module.exports = {
  description:
    "Split legacy project documents into projects, insights, and reports collections for the ml-v3 schema.",

  async up({ sourceDb, targetDb, batchSize, dryRun, logger }) {
    const sourceProjects = sourceDb.collection("projects");
    const targetProjects = targetDb.collection("projects");
    const targetInsights = targetDb.collection("insights");
    const targetReports = targetDb.collection("reports");

    const unitUsageAvailable = await collectionExists(sourceDb, "unit_usage");
    if (!unitUsageAvailable) {
      logger.warn(
        "unit_usage collection not found in source; insights will omit transId references."
      );
    }

    const unitUsageCollection = unitUsageAvailable
      ? sourceDb.collection("unit_usage")
      : null;
    const unitUsageIndex = await buildUnitUsageIndex(
      unitUsageCollection,
      batchSize,
      logger
    );

    const cursor = sourceProjects.find({}).batchSize(batchSize);

    let projectCount = 0;
    let insightCount = 0;
    let reportCount = 0;

    for await (const project of cursor) {
      const transformedProject = transformProject(project);

      if (dryRun) {
        logger.info(
          `Dry run - would upsert project ${describeId(transformedProject._id)}`
        );
      } else {
        await targetProjects.updateOne(
          { _id: transformedProject._id },
          { $set: transformedProject },
          { upsert: true }
        );
      }

      projectCount += 1;
      if (projectCount % batchSize === 0) {
        logger.info(`Processed ${projectCount} projects so far`);
      }

      const insightDoc = buildInsightDoc(project, unitUsageIndex, logger);
      if (insightDoc) {
        if (dryRun) {
          logger.info(
            `Dry run - would upsert insight ${describeId(
              insightDoc._id
            )} for project ${describeId(project._id)}`
          );
        } else {
          await targetInsights.updateOne(
            { _id: insightDoc._id },
            { $set: insightDoc },
            { upsert: true }
          );
        }

        insightCount += 1;
      }

      const reportDoc = buildReportDoc(project);
      if (reportDoc) {
        if (dryRun) {
          logger.info(
            `Dry run - would upsert report ${describeId(
              reportDoc._id
            )} for project ${describeId(project._id)}`
          );
        } else {
          await targetReports.updateOne(
            { _id: reportDoc._id },
            { $set: reportDoc },
            { upsert: true }
          );
        }

        reportCount += 1;
      }
    }

    logger.info(
      `Finished migrating projects. Projects: ${projectCount}, insights: ${insightCount}, reports: ${reportCount}.`
    );
  },
};

async function collectionExists(db, name) {
  if (!db) {
    return false;
  }

  const collections = await db
    .listCollections({ name }, { nameOnly: true })
    .toArray();
  return collections.length > 0;
}

const PROJECT_EXCLUDED_KEYS = new Set([
  "video",
  "video_url",
  "videoUrl",
  "videoMetadata",
  "video_metadata",
  "audio",
  "audio_url",
  "audioUrl",
  "speech_to_text",
  "speechToText",
  "true_value_score",
  "trueValueScore",
  "true_value_score_raw",
  "trueValueScoreRaw",
  "thumbnail",
  "thumbnails",
  "questions",
  "isSaveEmbeddings",
  "isUnitDeduct",
  "report",
  "message",
  "sucess",
  "success",
  "failure",
  "deleted",
  "transId",
]);

function transformProject(project) {
  const transformed = {};

  for (const [key, value] of Object.entries(project)) {
    if (!PROJECT_EXCLUDED_KEYS.has(key)) {
      transformed[key] = value;
    }
  }

  transformed._id = project._id;
  transformed.userId = project.userId;
  transformed.projectName = project.projectName;

  const description =
    project.projectDescription ||
    project.project_description ||
    project.projectSummary;
  if (description !== undefined) {
    transformed.projectDescription = description;
  } else {
    delete transformed.projectDescription;
  }

  if (project.type !== undefined) {
    transformed.type = project.type;
  }

  const link =
    project.link ||
    project.video_url ||
    project.video?.file ||
    project.audio_url;
  if (link) {
    transformed.link = link;
  } else {
    delete transformed.link;
  }

  if (typeof project.isDeleted === "boolean") {
    transformed.isDeleted = project.isDeleted;
  } else if (typeof project.deleted === "boolean") {
    transformed.isDeleted = project.deleted;
  } else if (
    typeof transformed.isDeleted !== "boolean" &&
    typeof transformed.deleted === "boolean"
  ) {
    transformed.isDeleted = transformed.deleted;
  }

  delete transformed.deleted;

  if ("createdAt" in transformed || project.createdAt) {
    const createdAt = ensureDate(project.createdAt || transformed.createdAt);
    if (createdAt) {
      transformed.createdAt = createdAt;
    } else {
      delete transformed.createdAt;
    }
  }

  if ("updatedAt" in transformed || project.updatedAt) {
    const updatedAt = ensureDate(project.updatedAt || transformed.updatedAt);
    if (updatedAt) {
      transformed.updatedAt = updatedAt;
    } else {
      delete transformed.updatedAt;
    }
  }

  if (project.__v !== undefined) {
    transformed.__v = project.__v;
  }

  for (const key of Object.keys(transformed)) {
    if (transformed[key] === undefined) {
      delete transformed[key];
    }
  }

  return transformed;
}

function buildInsightDoc(project, unitUsageIndex, logger) {
  const insightId = deriveDeterministicObjectId(project._id, 1);
  const projectObjectId = toObjectId(project._id);

  const insight = {
    _id: insightId,
    insightsOf:
      projectObjectId instanceof ObjectId ? projectObjectId : project._id,
    insightsOfType: "instavidq",
    video: buildVideo(project),
    audio: buildAudio(project),
    speechToText: buildSpeechToText(project),
    trueValueScore: buildTrueValueScore(project),
    thumbnails: buildThumbnails(project),
    chatQuestions: buildChatQuestions(project),
    embeddingsGenerated: Boolean(project.isSaveEmbeddings),
    createdAt: ensureDate(project.createdAt),
    updatedAt: ensureDate(project.updatedAt),
    isUnitDeduct: determineIsUnitDeduct(project),
  };

  cleanEmptySections(insight, [
    "video",
    "audio",
    "speechToText",
    "trueValueScore",
    "thumbnails",
    "chatQuestions",
  ]);

  applySectionDefaults(insight, [
    "video",
    "audio",
    "speechToText",
    "trueValueScore",
    "thumbnails",
  ]);

  const transId = resolveTransIdForProject(project, unitUsageIndex, logger);
  if (transId) {
    insight.transId = transId;
  }

  if (
    isEmptyValue(insight.video) &&
    isEmptyValue(insight.audio) &&
    isEmptyValue(insight.speechToText) &&
    isEmptyValue(insight.trueValueScore) &&
    isEmptyValue(insight.thumbnails) &&
    isEmptyValue(insight.chatQuestions)
  ) {
    return null;
  }

  return insight;
}

function buildReportDoc(project) {
  const sourceReport = project.report;
  if (!sourceReport || typeof sourceReport !== "object") {
    return null;
  }

  const normalized = { ...sourceReport };
  const projectObjectId = toObjectId(project._id);
  const consumedKeys = new Set([
    "reportOfType",
    "report_of_type",
    "duration",
    "emotionAnalysis",
    "emotion_analysis_llama",
    "emotion_analysis",
    "emotionGraph",
    "emotion_graph",
    "opinionSnippet",
    "opinion_snippet",
    "toneAnalysis",
    "tone_analysis",
    "engagementLevel",
    "engagement_level",
    "actionIntentions",
    "action_intentions",
    "verdict",
    "contrastingOpinions",
    "contrasting_opinions",
    "brandRecall",
    "brand_recall",
    "summary",
    "title",
    "category",
    "category_report",
    "status",
    "message",
    "createdAt",
    "updatedAt",
  ]);

  const report = {
    _id: deriveDeterministicObjectId(project._id, 2),
    reportOfId:
      projectObjectId instanceof ObjectId ? projectObjectId : project._id,
    reportOfType: "instavidq",
    duration: getFirstDefined(normalized, ["duration"], null),
    emotionAnalysis:
      getFirstDefined(
        normalized,
        ["emotionAnalysis", "emotion_analysis_llama", "emotion_analysis"],
        []
      ) || [],
    emotionGraph:
      getFirstDefined(normalized, ["emotionGraph", "emotion_graph"], {}) || {},
    opinionSnippet:
      getFirstDefined(normalized, ["opinionSnippet", "opinion_snippet"], []) ||
      [],
    toneAnalysis:
      getFirstDefined(normalized, ["toneAnalysis", "tone_analysis"], {}) || {},
    engagementLevel:
      getFirstDefined(
        normalized,
        ["engagementLevel", "engagement_level"],
        {}
      ) || {},
    actionIntentions: getFirstDefined(
      normalized,
      ["actionIntentions", "action_intentions"],
      null
    ),
    verdict: getFirstDefined(normalized, ["verdict"], null),
    contrastingOpinions: getFirstDefined(
      normalized,
      ["contrastingOpinions", "contrasting_opinions"],
      null
    ),
    brandRecall:
      getFirstDefined(normalized, ["brandRecall", "brand_recall"], {}) || {},
    summary: getFirstDefined(normalized, ["summary"], null),
    title: getFirstDefined(normalized, ["title"], project.projectName || null),
    category: getFirstDefined(normalized, ["category"], null),
    category_report: getFirstDefined(normalized, ["category_report"], {}) || {},
    status: getFirstDefined(normalized, ["status"], 2),
    message: getFirstDefined(normalized, ["message"], project.message || ""),
    createdAt:
      ensureDate(getFirstDefined(normalized, ["createdAt"], null)) ||
      ensureDate(project.createdAt),
    updatedAt:
      ensureDate(getFirstDefined(normalized, ["updatedAt"], null)) ||
      ensureDate(project.updatedAt),
  };

  for (const key of Object.keys(normalized)) {
    if (!consumedKeys.has(key) && report[key] === undefined) {
      report[key] = normalized[key];
    }
  }

  return report;
}

function buildVideo(project) {
  const video = project.video || {};
  const metadata = project.videoMetadata || project.video_metadata || {};

  const bucketUrl = project.video_url || video.url || video.bucketUrl || null;
  const title =
    project.report?.title || project.projectName || video.title || null;
  const duration =
    metadata.duration ??
    project.report?.duration ??
    project.speech_to_text?.duration ??
    project.speechToText?.duration ??
    video.duration ??
    null;
  const height = metadata.height ?? project.video?.height ?? null;
  const width = metadata.width ?? project.video?.width ?? null;

  const data = pruneUndefined({
    bucketUrl,
    title,
    duration,
    height,
    width,
  });

  const section = {};

  if (video.status !== undefined) {
    section.status = video.status;
  }

  if (!isEmptyValue(data)) {
    section.data = data;
  }

  if (video.message !== undefined) {
    section.message = video.message;
  }

  return section;
}

function buildAudio(project) {
  const audio = project.audio || {};

  const bucketUrl = project.audio_url || audio.url || audio.bucketUrl || null;
  const sourceUrl = audio.sourceUrl || audio.sourceFile || audio.file || null;

  const data = pruneUndefined({
    bucketUrl,
    sourceUrl,
  });

  const section = {};

  if (audio.status !== undefined) {
    section.status = audio.status;
  }

  if (!isEmptyValue(data)) {
    section.data = data;
  }

  if (audio.message !== undefined) {
    section.message = audio.message;
  }

  return section;
}

function buildSpeechToText(project) {
  const speech = project.speech_to_text || project.speechToText || {};

  return pruneUndefined({
    status: speech.status,
    data: speech.data,
    duration: speech.duration,
    message: speech.message,
    emotionSummary: speech.emotionSummary,
  });
}

function buildTrueValueScore(project) {
  const source = project.true_value_score || project.trueValueScore;
  const raw = project.true_value_score_raw || project.trueValueScoreRaw;

  const rawEntries = normalizeTrueValueScoreRaw(raw);

  const data = {};
  let hasData = false;
  let status;

  if (isPlainObject(source)) {
    if (source.status !== undefined) {
      status = source.status;
    }

    if (isPlainObject(source.data)) {
      Object.assign(data, source.data);
      if (Object.keys(source.data).length > 0) {
        hasData = true;
      }
    } else if (source.data !== undefined) {
      data.value = source.data;
      hasData = true;
    }

    const fallback = extractDataFallback(source);
    if (fallback) {
      Object.assign(data, fallback);
      hasData = true;
    }
  } else if (source !== undefined && source !== null) {
    data.value = source;
    hasData = true;
  }

  if (rawEntries.length > 0) {
    data.trueValueScoreRaw = rawEntries;
    hasData = true;
  }

  const result = {};

  if (hasData) {
    result.data = data;
  }

  if (status !== undefined) {
    result.status = status;
  }

  if (isPlainObject(source) && source.message !== undefined) {
    result.message = source.message;
  }

  return result;
}

function extractDataFallback(source) {
  if (!isPlainObject(source)) {
    return null;
  }

  const clone = { ...source };
  delete clone.status;
  delete clone.data;

  return Object.keys(clone).length > 0 ? clone : null;
}

function normalizeTrueValueScoreRaw(raw) {
  if (raw === undefined || raw === null) {
    return [];
  }

  if (Array.isArray(raw)) {
    return raw.map(entry =>
      typeof entry === "string" ? entry : JSON.stringify(entry)
    );
  }

  if (typeof raw === "string") {
    return [raw];
  }

  return [JSON.stringify(raw)];
}

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function buildThumbnails(project) {
  const thumbnail = project.thumbnail || project.thumbnails;
  if (!thumbnail) {
    return {};
  }

  const data = pruneUndefined({
    ...(thumbnail.files || thumbnail.data),
    processedAt: ensureDate(thumbnail.processedAt),
  });

  const section = {};

  if (!isEmptyValue(data)) {
    section.data = data;
  }

  if (thumbnail.status !== undefined) {
    section.status = thumbnail.status;
  }

  if (thumbnail.message !== undefined) {
    section.message = thumbnail.message;
  }

  return section;
}

function buildChatQuestions(project) {
  const questions = project.questions || [];
  const status = Array.isArray(questions) && questions.length > 0 ? 2 : 0;

  return {
    status,
    data: questions,
  };
}

function determineIsUnitDeduct(project) {
  if (typeof project.isUnitDeduct === "boolean") {
    return project.isUnitDeduct;
  }

  if (typeof project.sucess === "boolean") {
    return project.sucess;
  }

  if (typeof project.failure === "boolean") {
    return !project.failure;
  }

  return false;
}

async function buildUnitUsageIndex(collection, batchSize, logger) {
  if (!collection) {
    return { map: new Map(), ids: new Set() };
  }

  const map = new Map();
  const ids = new Set();

  const cursor = collection
    .find(
      {},
      {
        projection: {
          projectId: 1,
          project_id: 1,
          projectIdRef: 1,
          project_id_ref: 1,
          collectionRef: 1,
        },
      }
    )
    .batchSize(batchSize);

  let processed = 0;

  for await (const doc of cursor) {
    const usageIdHex = toHexString(doc._id);
    if (usageIdHex) {
      ids.add(usageIdHex);
    }

    const refs = gatherProjectRefs(doc);
    for (const ref of refs) {
      if (ref && !map.has(ref)) {
        map.set(ref, doc._id);
      }
    }

    processed += 1;
  }

  logger.info(
    `Indexed ${processed} unit_usage documents for reference lookups.`
  );

  return { map, ids };
}

function gatherProjectRefs(doc) {
  const refs = [];

  const pushRef = value => {
    const normalized = normalizeProjectReference(value);
    if (normalized) {
      refs.push(normalized);
    }
  };

  pushRef(doc.projectId);
  pushRef(doc.project_id);
  pushRef(doc.projectIdRef);
  pushRef(doc.project_id_ref);
  pushRef(doc.collectionRef);
  if (doc.collectionRef && doc.collectionRef.id) {
    pushRef(doc.collectionRef.id);
  }

  return refs;
}

function ensureDate(value) {
  if (!value) {
    return value;
  }

  if (value instanceof Date) {
    return value;
  }

  if (typeof value === "object" && value.$date) {
    return new Date(value.$date);
  }

  const date = new Date(value);
  if (!Number.isNaN(date.getTime())) {
    return date;
  }

  return value;
}

function pruneUndefined(obj) {
  const result = {};

  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) {
      result[key] = value;
    }
  }

  return result;
}

function cleanEmptySections(doc, keys) {
  for (const key of keys) {
    if (isEmptyValue(doc[key])) {
      delete doc[key];
    }
  }
}

function applySectionDefaults(target, keys) {
  if (!target) {
    return;
  }

  for (const key of keys) {
    const section = target[key];
    if (!section || typeof section !== "object") {
      continue;
    }

    if (!Object.prototype.hasOwnProperty.call(section, "data")) {
      continue;
    }

    const { data } = section;
    if (!data || typeof data !== "object" || Array.isArray(data)) {
      continue;
    }

    if (!Object.prototype.hasOwnProperty.call(section, "status")) {
      section.status = null;
    }

    if (!Object.prototype.hasOwnProperty.call(section, "message")) {
      section.message = null;
    }
  }
}

function resolveTransIdForProject(project, unitUsageIndex, logger) {
  if (!project) {
    return null;
  }

  const references = collectProjectReferenceCandidates(project);
  if (unitUsageIndex && unitUsageIndex.map) {
    for (const ref of references) {
      if (unitUsageIndex.map.has(ref)) {
        return unitUsageIndex.map.get(ref);
      }
    }
  }

  if (project.transId) {
    const normalized = toObjectId(project.transId);
    if (normalized instanceof ObjectId) {
      return normalized;
    }

    if (typeof normalized === "string") {
      if (ObjectId.isValid(normalized)) {
        return new ObjectId(normalized);
      }

      return normalized;
    }
  }

  if (logger && references.length > 0) {
    logger.debug?.(
      `unit_usage transId not found for project ${describeId(project._id)}`
    );
  }

  return null;
}

function collectProjectReferenceCandidates(project) {
  const refs = [];
  const seen = new Set();

  const pushRef = value => {
    const normalized = normalizeProjectReference(value);
    if (normalized && !seen.has(normalized)) {
      refs.push(normalized);
      seen.add(normalized);
    }
  };

  pushRef(project._id);
  pushRef(project.projectId);
  pushRef(project.project_id);
  pushRef(project.projectIdRef);
  pushRef(project.project_id_ref);

  if (project.collectionRef) {
    pushRef(project.collectionRef);
    pushRef(project.collectionRef.id);
  }

  return refs;
}

function normalizeProjectReference(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (value && typeof value === "object" && value.id !== undefined) {
    return normalizeProjectReference(value.id);
  }

  const hex = toHexString(value);
  if (hex) {
    return hex;
  }

  if (typeof value === "number") {
    return String(value);
  }

  if (typeof value === "string" && value.trim() !== "") {
    return value;
  }

  return null;
}

function getFirstDefined(obj, keys, defaultValue = undefined) {
  for (const key of keys) {
    if (
      Object.prototype.hasOwnProperty.call(obj, key) &&
      obj[key] !== undefined
    ) {
      return obj[key];
    }
  }

  return defaultValue;
}

function isEmptyValue(value) {
  if (value === null || value === undefined) {
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

function toHexString(value) {
  if (!value) {
    return null;
  }

  if (value instanceof ObjectId) {
    return value.toHexString();
  }

  if (typeof value === "string") {
    if (ObjectId.isValid(value)) {
      return new ObjectId(value).toHexString();
    }

    return value;
  }

  if (typeof value === "object" && value.$oid && ObjectId.isValid(value.$oid)) {
    return value.$oid;
  }

  return null;
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
