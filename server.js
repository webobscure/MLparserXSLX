"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const dotenv = require("dotenv");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");

const swaggerUi = require("swagger-ui-express");
const swaggerJSDoc = require("swagger-jsdoc");

const { sendResultEmail } = require("./mailer");

const app = express();
dotenv.config();
app.set("trust proxy", 1);
app.use(cors());

//  Configs
const reqCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/required.json"), "utf-8")
);

const modelsCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/models.json"), "utf-8")
);

//  Upload
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB исходный файл (base64 будет больше!)
  fileFilter: (req, file, cb) => {
    const ok =
      file.mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      file.mimetype === "application/vnd.ms-excel" ||
      file.originalname.toLowerCase().endsWith(".xlsx") ||
      file.originalname.toLowerCase().endsWith(".xls");
    cb(ok ? null : new Error("Only Excel files (.xlsx/.xls) are allowed"), ok);
  },
});

// Helpers: parsing + mapping
function norm(s) {
  return String(s ?? "")
    .trim()
    .toLowerCase()
    .replace(/[\u00A0]/g, " ")
    .replace(/[_\-]+/g, " ")
    .replace(/[^\p{L}\p{N} ]/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

function safeJsonParse(str, fallback) {
  try {
    return JSON.parse(str);
  } catch {
    return fallback;
  }
}

function extractColumnsFromFirstSheet(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const aoa = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
    blankrows: false,
  });

  // first non-empty row as headers
  let headerRow = 0;
  for (let i = 0; i < aoa.length; i++) {
    if ((aoa[i] || []).some((v) => norm(v) !== "")) {
      headerRow = i;
      break;
    }
  }

  return (aoa[headerRow] || []).map((v) => String(v).trim()).filter(Boolean);
}

function autoMap(headers) {
  const headersInfo = headers
    .map((h) => ({ raw: String(h).trim(), n: norm(h) }))
    .filter((h) => h.raw);

  function findOne(key) {
    const aliases = (reqCfg.aliases?.[key] || []).map(norm);

    const exact = headersInfo.find((h) => aliases.includes(h.n));
    if (exact) return exact.raw;

    const partial = headersInfo.find((h) =>
      aliases.some((a) => h.n.includes(a))
    );
    return partial ? partial.raw : null;
  }

  function findMany(key) {
    const aliases = (reqCfg.aliases?.[key] || []).map(norm);
    const hits = headersInfo
      .filter((h) => aliases.some((a) => h.n === a || h.n.includes(a)))
      .map((h) => h.raw);
    return Array.from(new Set(hits));
  }

  const mapping = {
    product_images: findOne("product_images"),
    title: findOne("title"),
    description: findOne("description"),
    bullet_points: findMany("bullet_points"),
  };

  const missing = [];
  if (!mapping.product_images) missing.push("product_images");
  if (!mapping.title) missing.push("title");
  if (!mapping.description) missing.push("description");
  if (!mapping.bullet_points || mapping.bullet_points.length === 0)
    missing.push("bullet_points");

  return { mapping, missing };
}

function candidatesFor(headers, key, limit = 10) {
  const aliases = (reqCfg.aliases?.[key] || []).map(norm);

  const scored = headers
    .map((h) => {
      const hn = norm(h);
      let score = 0;

      if (aliases.includes(hn)) score = 3;
      else if (aliases.some((a) => hn.includes(a))) score = 2;
      else {
        const tokens = aliases.flatMap((a) => a.split(" ")).filter(Boolean);
        if (tokens.some((t) => hn.includes(t))) score = 1;
      }

      return { header: h, score };
    })
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score || a.header.localeCompare(b.header))
    .map((x) => x.header);

  return Array.from(new Set(scored)).slice(0, limit);
}

function validateMapping(mapping, columns) {
  const normToRaw = new Map();
  for (const c of columns) normToRaw.set(norm(c), c);

  function exists(col) {
    if (!col) return null;
    return normToRaw.get(norm(col)) || null;
  }

  const errors = [];

  const product_images = exists(mapping.product_images);
  if (!product_images) errors.push(`Mapped column not found: ${mapping.product_images}`);

  const title = exists(mapping.title);
  if (!title) errors.push(`Mapped column not found: ${mapping.title}`);

  const description = exists(mapping.description);
  if (!description) errors.push(`Mapped column not found: ${mapping.description}`);

  const bpInput = Array.isArray(mapping.bullet_points)
    ? mapping.bullet_points
    : mapping.bullet_points ? [mapping.bullet_points] : [];

  const bullet_points = bpInput.map(exists).filter(Boolean);
  if (!bullet_points.length) errors.push(`Mapped bullet_points column not found: ${bpInput.join(", ")}`);

  return { ok: errors.length === 0, errors, bullet_points };
}


function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function isRetryableFetchError(err) {
  const msg = String(err?.message || "").toLowerCase();
  return (
    msg.includes("fetch failed") ||
    msg.includes("socket") ||
    msg.includes("econnreset") ||
    msg.includes("etimedout") ||
    msg.includes("timeout") ||
    msg.includes("network") ||
    msg.includes("undici")
  );
}

async function fetchJsonWithRetry(
  url,
  options,
  {
    retries = 5,
    timeoutMs = 60 * 60 * 1000, // 60 minutes
    baseDelayMs = 2000,
    maxDelayMs = 60_000,
  } = {}
) {
  let lastErr;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const r = await fetch(url, { ...options, signal: controller.signal });

      const text = await r.text();

      if (!r.ok) {
        const err = new Error(`HTTP ${r.status}: ${text}`);
        err.httpStatus = r.status;
        throw err;
      }

      const data = JSON.parse(text);
      return data;
    } catch (e) {
      lastErr = e;

      const status = e?.httpStatus;
      const retryableHttp = status && (status === 429 || status >= 500); // 429/5xx
      const retryableNet = isRetryableFetchError(e) || e?.name === "AbortError";

      if (attempt === retries || (!retryableHttp && !retryableNet)) {
        throw lastErr;
      }

      // backoff: 2s, 4s, 8s... + jitter
      const exp = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt - 1));
      const jitter = Math.floor(Math.random() * 500);
      const wait = exp + jitter;

      console.warn(
        `Inference attempt ${attempt} failed, retrying in ${wait}ms`,
        {
          error: String(e?.message || e),
          status,
        }
      );

      await sleep(wait);
    } finally {
      clearTimeout(t);
    }
  }

  throw lastErr;
}

// Modal inference client
async function callInferenceModal({ buffer, filename, modelsList }) {
  const url =
    process.env.INFERENCE_URL ||
    "https://dsitdvitamins--test-inference-predict.modal.run";
  const apiKey = process.env.INFERENCE_API_KEY;

  if (!apiKey) throw new Error("INFERENCE_API_KEY is not set");

  const xlsxBase64 = Buffer.from(buffer).toString("base64");

  const payload = {
    api_key: apiKey,
    xlsx_base64: xlsxBase64,
    models_list: modelsList,
    filename: filename || "input.xlsx",
  };

  const data = await fetchJsonWithRetry(
    url,
    {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json",
      },
      body: JSON.stringify(payload),
    },
    {
      retries: Number(process.env.INFERENCE_RETRIES || 5),
      timeoutMs: Number(process.env.INFERENCE_TIMEOUT_MS || 60 * 60 * 1000), // 60 min
      baseDelayMs: Number(process.env.INFERENCE_RETRY_BASE_DELAY_MS || 2000),
      maxDelayMs: Number(process.env.INFERENCE_RETRY_MAX_DELAY_MS || 60_000),
    }
  );

  if (!data?.ok) throw new Error(`Inference ok=false: ${JSON.stringify(data)}`);
  if (!data.xlsx_base64) throw new Error("Inference missing xlsx_base64");

  return data;
}

// Swagger (OpenAPI)
const swaggerSpec = swaggerJSDoc({
  definition: {
    openapi: "3.0.0",
    info: {
      title: "ML Parser XLSX API",
      version: "1.0.0",
      description:
        "Upload Excel, map required columns, choose prediction models, submit a job (sent to Modal inference API), and receive the result by email.",
    },
    servers: [{ url: "/" }],
  },
  apis: [],
});

swaggerSpec.paths = {
  "/": {
    get: {
      summary: "Healthcheck",
      responses: {
        200: {
          description: "OK",
          content: {
            "text/plain": { schema: { type: "string", example: "OK" } },
          },
        },
      },
    },
  },

  "/api/inspect": {
    post: {
      summary: "Inspect Excel columns and auto-map required fields",
      requestBody: {
        required: true,
        content: {
          "multipart/form-data": {
            schema: {
              type: "object",
              properties: { file: { type: "string", format: "binary" } },
              required: ["file"],
            },
          },
        },
      },
      responses: {
        200: {
          description: "Inspection result",
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/InspectResponse" },
            },
          },
        },
        400: {
          description: "Bad request",
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/ErrorResponse" },
            },
          },
        },
      },
    },
  },

  "/api/models": {
    get: {
      summary: "List available prediction models",
      responses: {
        200: {
          description: "Models list",
          content: {
            "application/json": {
              schema: {
                type: "array",
                items: { $ref: "#/components/schemas/Model" },
              },
            },
          },
        },
      },
    },
  },

  "/api/jobs": {
    post: {
      summary:
        "Create job (sent to Modal inference API, result delivered by email)",
      description:
        "Validates mapping + models, uploads input file, calls Modal inference API, and emails resulting XLSX.\n\n" +
        "IMPORTANT: mapping/models are JSON strings in multipart/form-data.",
      requestBody: {
        required: true,
        content: {
          "multipart/form-data": {
            schema: {
              type: "object",
              properties: {
                file: { type: "string", format: "binary" },
                email: { type: "string", example: "user@example.com" },
                mapping: {
                  type: "string",
                  example:
                    '{"product_images":"Product Images","title":"Title","description":"Description","bullet_points":"Bullet Points"}',
                },
                models: { type: "string", example: '["braket_type"]' },
              },
              required: ["file", "email", "mapping", "models"],
            },
          },
        },
      },
      responses: {
        201: {
          description: "Job created",
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/JobResponse" },
            },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": {
              schema: { $ref: "#/components/schemas/ErrorResponse" },
            },
          },
        },
      },
    },
  },
};

swaggerSpec.components = {
  schemas: {
    Model: {
      type: "object",
      properties: {
        id: { type: "string", example: "braket_type" },
        title: { type: "string", example: "Bracket type" },
      },
      required: ["id", "title"],
    },

    InspectAutoMapping: {
      type: "object",
      properties: {
        product_images: { type: ["string", "null"] },
        title: { type: ["string", "null"] },
        description: { type: ["string", "null"] },
        bullet_points: { type: ["string", "null"] },
      },
      required: ["product_images", "title", "description", "bullet_points"],
    },

    InspectResponse: {
      type: "object",
      properties: {
        fileToken: { type: "string" },
        columns: { type: "array", items: { type: "string" } },
        required: { type: "array", items: { type: "string" } },
        autoMapping: { $ref: "#/components/schemas/InspectAutoMapping" },
        missing: { type: "array", items: { type: "string" } },
        candidates: {
          type: "object",
          additionalProperties: { type: "array", items: { type: "string" } },
        },
      },
      required: [
        "fileToken",
        "columns",
        "required",
        "autoMapping",
        "missing",
        "candidates",
      ],
    },

    JobResponse: {
      type: "object",
      properties: {
        jobId: { type: "string", example: "job_3a8f2c1d9b4e7f10" },
        status: { type: "string", example: "queued" },
      },
      required: ["jobId", "status"],
    },

    ErrorResponse: {
      type: "object",
      properties: {
        error: { type: "string" },
        details: {
          oneOf: [
            { type: "string" },
            { type: "array", items: { type: "string" } },
            { type: "object" },
          ],
        },
      },
      required: ["error"],
    },
  },
};

app.get("/openapi.json", (req, res) => res.json(swaggerSpec));
app.use(
  "/docs",
  swaggerUi.serve,
  swaggerUi.setup(swaggerSpec, { explorer: true })
);

// Routes
app.get("/", (req, res) => res.send("OK"));

// Step 1: inspect
app.post("/api/inspect", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file" });

  const columns = extractColumnsFromFirstSheet(req.file.buffer);
  const { mapping, missing } = autoMap(columns);

  const candidates = {};
  for (const key of reqCfg.required) {
    candidates[key] = candidatesFor(
      columns,
      key,
      key === "bullet_points" ? 20 : 10
    );
  }

  const fileToken = "f_" + crypto.randomBytes(16).toString("hex");

  return res.json({
    fileToken,
    columns,
    required: reqCfg.required,
    autoMapping: mapping,
    missing,
    candidates,
  });
});

// Step 2: models
app.get("/api/models", (req, res) => {
  res.json(modelsCfg);
});

// Step 3: jobs -> call Modal -> email result
app.post("/api/jobs", upload.single("file"), async (req, res) => {
  try {
    const email = String(req.body.email || "").trim();
    const mapping = safeJsonParse(req.body.mapping || "{}", {});
    let models = safeJsonParse(req.body.models || "[]", []);
    if (!Array.isArray(models) || !models.length) models = ["braket_type"];

    if (!req.file) return res.status(400).json({ error: "No file" });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
      return res.status(400).json({ error: "Invalid email" });

    const columns = extractColumnsFromFirstSheet(req.file.buffer);

    const v = validateMapping(mapping, columns);
    if (!v.ok)
      return res
        .status(400)
        .json({ error: "Invalid mapping", details: v.errors });
    mapping.bullet_points = v.bullet_points;

    const allowed = new Set(modelsCfg.map((m) => m.id));
    for (const m of models) {
      if (!allowed.has(m)) {
        return res.status(400).json({
          error: `Hardcoded model is not in configs/models.json: ${m}`,
        });
      }
    }
    if (!models.length)
      return res.status(400).json({ error: "No models selected" });

    const jobId = "job_" + crypto.randomBytes(8).toString("hex");

    // быстро отвечаем фронту
    res.status(201).json({ jobId, status: "queued" });

    // в фоне: инференс -> письмо
    setImmediate(async () => {
      await sendResultEmail({
        to: email,
        subject: "⏳ Your file is being processed",
        text: `Job ${jobId} has started.\nWe will email you when it is ready.`,
      });
      const reqFilename = req.file.originalname || "input.xlsx";

      let inferenceResp;
      try {
        inferenceResp = await callInferenceModal({
          buffer: req.file.buffer,
          filename: reqFilename,
          modelsList: models,
        });
        console.log("✅ Inference OK", { jobId, n_rows: inferenceResp.n_rows });
      } catch (e) {
        console.error("❌ Inference failed", { jobId, error: e?.message || e });
        return;
      }

      try {
        const resultBuf = Buffer.from(inferenceResp.xlsx_base64, "base64");
        const outName = inferenceResp.filename || `result-${jobId}.xlsx`;

        await sendResultEmail({
          to: email,
          subject: "Your file is ready",
          text: `Done. JobId: ${jobId}\nRows: ${inferenceResp.n_rows ?? "?"}`,
          filename: outName,
          contentBuffer: resultBuf,
        });

        console.log("✅ Email sent", { jobId, email });
      } catch (e) {
        console.error("❌ Email failed", {
          jobId,
          error: e?.message || e,
          name: e?.name,
          code: e?.code,
          response: e?.response,
        });
      }
    });
  } catch (e) {
    return res.status(400).json({ error: "Bad request", details: e.message });
  }
});

//  Process error handlers
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));
process.on("unhandledRejection", (e) =>
  console.error("unhandledRejection:", e)
);

async function sendStartupEmailOnce() {
  try {
    if (!process.env.STARTUP_NOTIFY_EMAIL) {
      console.log("ℹ️ STARTUP_NOTIFY_EMAIL not set, skip startup email");
      return;
    }

    await sendResultEmail({
      to: process.env.STARTUP_NOTIFY_EMAIL,
      subject: "🚀 ML Parser service started",
      text: [
        "Service startup notification",
        "",
        `Time: ${new Date().toISOString()}`,
        `Node: ${process.version}`,
        `PID: ${process.pid}`,
        `Env: ${process.env.NODE_ENV || "unknown"}`,
      ].join("\n"),
    });

    console.log("📨 Startup email sent");
  } catch (e) {
    console.error("❌ Failed to send startup email:", e?.message || e);
  }
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log("Listening on", PORT);
  sendStartupEmailOnce();
});
