"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");

const swaggerUi = require("swagger-ui-express");
const swaggerJSDoc = require("swagger-jsdoc");

const { sendResultEmail } = require("./mailer");

const app = express();
app.set("trust proxy", 1);
app.use(cors());

// -------------------------
// Configs
// -------------------------
const reqCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/required.json"), "utf-8")
);

const modelsCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/models.json"), "utf-8")
);

// -------------------------
// Temp file storage (for Runpod to download)
// -------------------------
const TEMP_FILES = new Map(); // token -> { buffer, filename, mime, expiresAt }
const TEMP_TTL_MS = Number(process.env.TEMP_TTL_MS || 60 * 60 * 1000); // 60 min by default

function putTempFile({ buffer, filename, mime }) {
  const token = crypto.randomBytes(24).toString("hex");
  TEMP_FILES.set(token, {
    buffer,
    filename: filename || "input.xlsx",
    mime: mime || "application/octet-stream",
    expiresAt: Date.now() + TEMP_TTL_MS,
  });
  return token;
}

// cleanup
setInterval(() => {
  const now = Date.now();
  for (const [t, v] of TEMP_FILES.entries()) {
    if (v.expiresAt <= now) TEMP_FILES.delete(t);
  }
}, 60_000);

// Route for ML/Runpod to fetch file by token
app.get("/api/tmp/:token", (req, res) => {
  const item = TEMP_FILES.get(req.params.token);
  if (!item || item.expiresAt <= Date.now()) return res.sendStatus(404);

  res.setHeader("Content-Type", item.mime);
  res.setHeader("Content-Disposition", `attachment; filename="${item.filename}"`);
  return res.send(item.buffer);
});

function publicBaseUrl(req) {
  const proto = req.headers["x-forwarded-proto"] || req.protocol;
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}`;
}

// -------------------------
// Multer (Excel upload)
// -------------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB
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

// -------------------------
// Helpers: parsing + mapping
// -------------------------
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

    const partial = headersInfo.find((h) => aliases.some((a) => h.n.includes(a)));
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
  if (!mapping.bullet_points || mapping.bullet_points.length === 0) missing.push("bullet_points");

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
  const set = new Set(columns);

  const bp = Array.isArray(mapping.bullet_points)
    ? mapping.bullet_points
    : mapping.bullet_points
    ? [mapping.bullet_points]
    : [];

  const errors = [];

  if (!mapping.product_images) errors.push("Missing mapping: product_images");
  else if (!set.has(mapping.product_images))
    errors.push(`Mapped column not found: ${mapping.product_images}`);

  if (!mapping.title) errors.push("Missing mapping: title");
  else if (!set.has(mapping.title))
    errors.push(`Mapped column not found: ${mapping.title}`);

  if (!mapping.description) errors.push("Missing mapping: description");
  else if (!set.has(mapping.description))
    errors.push(`Mapped column not found: ${mapping.description}`);

  if (!bp.length) errors.push("Missing mapping: bullet_points");
  else {
    for (const col of bp) {
      if (!set.has(col)) errors.push(`Mapped bullet_points column not found: ${col}`);
    }
  }

  return { ok: errors.length === 0, errors, bullet_points: bp };
}

// -------------------------
// Runpod client
// -------------------------
async function runpodRun({ fileUrl, mapping, models }) {
  const endpointId = process.env.RUNPOD_ENDPOINT_ID;
  const apiKey = process.env.RUNPOD_API_KEY;
  const base = process.env.RUNPOD_BASE_URL || "https://api.runpod.ai/v2";

  if (!endpointId) throw new Error("RUNPOD_ENDPOINT_ID is not set");
  if (!apiKey) throw new Error("RUNPOD_API_KEY is not set");

  const r = await fetch(`${base}/${endpointId}/run`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      accept: "application/json",
      authorization: apiKey,
    },
    body: JSON.stringify({
      input: { file_url: fileUrl, mapping, models },
    }),
  });

  if (!r.ok) throw new Error(`Runpod /run failed: ${r.status} ${await r.text()}`);
  return await r.json(); // expect { id, status, ... }
}

async function runpodStatus(runpodJobId) {
  const endpointId = process.env.RUNPOD_ENDPOINT_ID;
  const apiKey = process.env.RUNPOD_API_KEY;
  const base = process.env.RUNPOD_BASE_URL || "https://api.runpod.ai/v2";

  const r = await fetch(`${base}/${endpointId}/status/${runpodJobId}`, {
    headers: { authorization: apiKey, accept: "application/json" },
  });

  if (!r.ok) throw new Error(`Runpod /status failed: ${r.status} ${await r.text()}`);
  return await r.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitRunpod(runpodJobId, { intervalMs = 4000, timeoutMs = 20 * 60 * 1000 } = {}) {
  const started = Date.now();
  while (true) {
    const st = await runpodStatus(runpodJobId);

    if (st.status === "COMPLETED") return st;
    if (st.status === "FAILED" || st.status === "TIMED_OUT" || st.status === "CANCELLED") {
      throw new Error(`Runpod job ${st.status}: ${JSON.stringify(st.error || st)}`);
    }

    if (Date.now() - started > timeoutMs) throw new Error("Runpod wait timeout");
    await sleep(intervalMs);
  }
}

async function getResultBufferFromRunpodOutput(output) {
  // Contract options with ML:
  // 1) output.result_file_url  (best)
  // 2) output.result_file_base64
  if (!output) throw new Error("Runpod COMPLETED but output is empty");

  if (output.result_file_url) {
    const r = await fetch(output.result_file_url);
    if (!r.ok) throw new Error(`Failed to download result_file_url: ${r.status}`);
    const ab = await r.arrayBuffer();
    return Buffer.from(ab);
  }

  if (output.result_file_base64) {
    return Buffer.from(output.result_file_base64, "base64");
  }

  throw new Error("Unknown output format (need result_file_url or result_file_base64)");
}

// -------------------------
// Swagger (OpenAPI)
// -------------------------
const swaggerSpec = swaggerJSDoc({
  definition: {
    openapi: "3.0.0",
    info: {
      title: "ML Parser XLSX API",
      version: "1.0.0",
      description:
        "Upload Excel, map required columns, choose prediction models, submit a job (sent to Runpod), and receive the result by email.",
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
          content: { "text/plain": { schema: { type: "string", example: "OK" } } },
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
              properties: {
                file: { type: "string", format: "binary" },
              },
              required: ["file"],
            },
          },
        },
      },
      responses: {
        200: {
          description: "Inspection result",
          content: {
            "application/json": { schema: { $ref: "#/components/schemas/InspectResponse" } },
          },
        },
        400: {
          description: "Bad request",
          content: {
            "application/json": { schema: { $ref: "#/components/schemas/ErrorResponse" } },
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
              schema: { type: "array", items: { $ref: "#/components/schemas/Model" } },
            },
          },
        },
      },
    },
  },

  "/api/jobs": {
    post: {
      summary: "Create job (sent to Runpod, result delivered by email)",
      description:
        "Validates mapping + models, uploads input file, sends a Runpod /run request, returns jobId and runpodJobId.\n" +
        "Then backend waits for Runpod completion and emails the resulting file.\n\n" +
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
                    '{"product_images":"Product Images","title":"Title","description":"Description","bullet_points":["Bullet Point 1","Bullet Point 2"]}',
                },
                models: { type: "string", example: '["demand_forecast","stockout_risk"]' },
              },
              required: ["file", "email", "mapping", "models"],
            },
          },
        },
      },
      responses: {
        201: {
          description: "Job created (Runpod started)",
          content: {
            "application/json": { schema: { $ref: "#/components/schemas/JobResponse" } },
          },
        },
        400: {
          description: "Validation error",
          content: {
            "application/json": { schema: { $ref: "#/components/schemas/ErrorResponse" } },
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
        id: { type: "string", example: "demand_forecast" },
        title: { type: "string", example: "Demand forecast" },
      },
      required: ["id", "title"],
    },

    InspectAutoMapping: {
      type: "object",
      properties: {
        product_images: { type: ["string", "null"], example: "Product Images" },
        title: { type: ["string", "null"], example: "Title" },
        description: { type: ["string", "null"], example: "Description" },
        bullet_points: {
          type: "array",
          items: { type: "string" },
          example: ["Bullet Point 1", "Bullet Point 2"],
        },
      },
      required: ["product_images", "title", "description", "bullet_points"],
    },

    InspectResponse: {
      type: "object",
      properties: {
        fileToken: { type: "string", example: "f_4fd1c2a9d0b84c19a3c3a1b8b4c1e2aa" },
        columns: { type: "array", items: { type: "string" } },
        required: { type: "array", items: { type: "string" } },
        autoMapping: { $ref: "#/components/schemas/InspectAutoMapping" },
        missing: { type: "array", items: { type: "string" } },
        candidates: {
          type: "object",
          additionalProperties: { type: "array", items: { type: "string" } },
        },
      },
      required: ["fileToken", "columns", "required", "autoMapping", "missing", "candidates"],
    },

    JobResponse: {
      type: "object",
      properties: {
        jobId: { type: "string", example: "job_3a8f2c1d9b4e7f10" },
        status: { type: "string", example: "queued" },
        runpodJobId: { type: "string", example: "rp_1234567890abcdef" },
      },
      required: ["jobId", "status", "runpodJobId"],
    },

    ErrorResponse: {
      type: "object",
      properties: {
        error: { type: "string", example: "Invalid mapping" },
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
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec, { explorer: true }));

// -------------------------
// Routes
// -------------------------
app.get("/", (req, res) => res.send("OK"));

// Step 1: inspect
app.post("/api/inspect", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file" });

  const columns = extractColumnsFromFirstSheet(req.file.buffer);
  const { mapping, missing } = autoMap(columns);

  const candidates = {};
  for (const key of reqCfg.required) {
    candidates[key] = candidatesFor(columns, key, key === "bullet_points" ? 20 : 10);
  }

  const fileToken = "f_" + crypto.randomBytes(16).toString("hex"); // purely for frontend correlation

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

// Step 3: jobs -> send to Runpod -> email result
app.post("/api/jobs", upload.single("file"), async (req, res) => {
  try {
    const email = String(req.body.email || "").trim();
    const mapping = safeJsonParse(req.body.mapping || "{}", {});
    const models = safeJsonParse(req.body.models || "[]", []);

    if (!req.file) return res.status(400).json({ error: "No file" });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
      return res.status(400).json({ error: "Invalid email" });

    const columns = extractColumnsFromFirstSheet(req.file.buffer);

    const v = validateMapping(mapping, columns);
    if (!v.ok) return res.status(400).json({ error: "Invalid mapping", details: v.errors });
    mapping.bullet_points = v.bullet_points;

    const allowed = new Set(modelsCfg.map((m) => m.id));
    for (const m of models) {
      if (!allowed.has(m)) return res.status(400).json({ error: `Unknown model: ${m}` });
    }
    if (!models.length) return res.status(400).json({ error: "No models selected" });

    const jobId = "job_" + crypto.randomBytes(8).toString("hex");

    // store temp file for Runpod to download
    const token = putTempFile({
      buffer: req.file.buffer,
      filename: req.file.originalname || "input.xlsx",
      mime: req.file.mimetype,
    });
    const fileUrl = `${publicBaseUrl(req)}/api/tmp/${token}`;

    // start Runpod
    const runInfo = await runpodRun({ fileUrl, mapping, models });
    const runpodJobId = runInfo.id;

    // respond to frontend immediately
    res.status(201).json({ jobId, status: "queued", runpodJobId });

    // background: wait for result and email it
    setImmediate(async () => {
      try {
        const st = await waitRunpod(runpodJobId);
        const resultBuffer = await getResultBufferFromRunpodOutput(st.output);

        await sendResultEmail({
          to: email,
          subject: "Your file is ready",
          text: `Done. JobId: ${jobId}`,
          filename: `result-${jobId}.xlsx`,
          contentBuffer: resultBuffer,
        });

        console.log("✅ Completed & emailed:", { jobId, runpodJobId, email });
      } catch (e) {
        console.error("❌ Background job failed:", { jobId, runpodJobId, error: e?.message || e });
        // Optional: email failure notice here if you want
      }
    });
  } catch (e) {
    return res.status(400).json({ error: "Bad request", details: e.message });
  }
});

// -------------------------
// Process error handlers
// -------------------------
process.on("uncaughtException", (e) => console.error("uncaughtException:", e));
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));

// -------------------------
// Listen
// -------------------------
const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => console.log("Listening on", PORT));
