const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const swaggerUi = require("swagger-ui-express");
const swaggerJSDoc = require("swagger-jsdoc");


const app = express();
app.set("trust proxy", 1);
app.use(cors());

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 },
});

const reqCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/required.json"), "utf-8")
);
const modelsCfg = JSON.parse(
  fs.readFileSync(path.join(__dirname, "configs/models.json"), "utf-8")
);

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

function extractColumnsFromFirstSheet(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];
  const aoa = XLSX.utils.sheet_to_json(sheet, {
    header: 1,
    defval: "",
    raw: false,
    blankrows: false,
  });

  // первая непустая строка = заголовки
  let headerRow = 0;
  for (let i = 0; i < aoa.length; i++) {
    if ((aoa[i] || []).some((v) => norm(v) !== "")) {
      headerRow = i;
      break;
    }
  }
  const headers = (aoa[headerRow] || [])
    .map((v) => String(v).trim())
    .filter(Boolean);
  return headers;
}

function autoMap(headers) {
  const headersInfo = headers
    .map((h) => ({ raw: String(h).trim(), n: norm(h) }))
    .filter((h) => h.raw);

  function findOne(key) {
    const aliases = (reqCfg.aliases?.[key] || []).map(norm);
    // точное совпадение
    const exact = headersInfo.find((h) => aliases.includes(h.n));
    if (exact) return exact.raw;
    // частичное (contains)
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

      // score: 3 = exact alias, 2 = contains alias, 1 = contains any alias token
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

  // уникальные + лимит
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
      if (!set.has(col))
        errors.push(`Mapped bullet_points column not found: ${col}`);
    }
  }

  return { ok: errors.length === 0, errors, bullet_points: bp };
}

const swaggerSpec = swaggerJSDoc({
  definition: {
    openapi: "3.0.0",
    info: {
      title: "ML Parser XLSX API",
      version: "1.0.0",
      description:
        "Upload an Excel file, map required columns, choose prediction models, submit a job and get results by email.",
    },
    // важно: относительный сервер, чтобы работало и локально, и на Railway
    servers: [{ url: "/" }],
  },
  apis: [],
});

// Описываем API руками
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
      description:
        "Uploads Excel file, extracts headers, returns columns, autoMapping, missing required fields and candidates.",
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
            "application/json": {
              schema: { $ref: "#/components/schemas/InspectResponse" },
            },
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
      summary: "Create job (file + mapping + selected models + email)",
      description:
        "Uploads file again (simple approach), validates mapping for required fields, validates selected models, returns jobId.\n\n" +
        "IMPORTANT: `mapping` and `models` are sent as JSON strings in multipart/form-data.\n" +
        "Example mapping JSON:\n" +
        `{ "product_images":"Product Images","title":"Title","description":"Description","bullet_points":["Bullet Point 1","Bullet Point 2"] }`,
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
                  description: "JSON string with column mapping",
                },
                models: {
                  type: "string",
                  example: '["demand_forecast","stockout_risk"]',
                  description: "JSON string array of selected model ids",
                },
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
        columns: {
          type: "array",
          items: { type: "string" },
          example: ["Product Images", "Title", "Bullet Point 1", "Bullet Point 2", "Description"],
        },
        required: {
          type: "array",
          items: { type: "string" },
          example: ["product_images", "title", "bullet_points", "description"],
        },
        autoMapping: { $ref: "#/components/schemas/InspectAutoMapping" },
        missing: {
          type: "array",
          items: { type: "string" },
          example: [],
        },
        candidates: {
          type: "object",
          additionalProperties: {
            type: "array",
            items: { type: "string" },
          },
          example: {
            product_images: ["Product Images", "Image URL"],
            title: ["Title", "Product Title"],
            bullet_points: ["Bullet Point 1", "Bullet Point 2", "Key Features"],
            description: ["Description", "Product Description"],
          },
        },
      },
      required: ["fileToken", "columns", "required", "autoMapping", "missing", "candidates"],
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

// Роуты документации
app.get("/openapi.json", (req, res) => res.json(swaggerSpec));
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec, { explorer: true }));


// Шаг 1: inspect
app.post("/api/inspect", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ error: "No file" });

  const columns = extractColumnsFromFirstSheet(req.file.buffer);
  const { mapping, missing } = autoMap(columns);

  // fileToken (если хочешь хранить файл временно)
  const fileToken = "f_" + crypto.randomBytes(16).toString("hex");
  // Минимально: можно пока НЕ хранить файл и заставить фронт прислать файл снова на /api/jobs

  const candidates = {};
  for (const key of reqCfg.required) {
    candidates[key] = candidatesFor(
      columns,
      key,
      key === "bullet_points" ? 20 : 10
    );
  }

  res.json({
    fileToken,
    columns,
    required: reqCfg.required,
    autoMapping: mapping,
    missing,
    candidates,
  });
});
// Шаг 2: models list
app.get("/api/models", (req, res) => {
  res.json(modelsCfg);
});

// Шаг 3: jobs
app.post("/api/jobs", upload.single("file"), (req, res) => {
  try {
    const email = String(req.body.email || "").trim();
    const mapping = JSON.parse(req.body.mapping || "{}");
    const models = JSON.parse(req.body.models || "[]");

    if (!req.file) return res.status(400).json({ error: "No file" });
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email))
      return res.status(400).json({ error: "Invalid email" });

    const columns = extractColumnsFromFirstSheet(req.file.buffer);

    // validate mapping covers required
    const v = validateMapping(mapping, columns);
    if (!v.ok)
      return res
        .status(400)
        .json({ error: "Invalid mapping", details: v.errors });

    // нормализуем bullet_points (чтобы дальше было удобно)
    mapping.bullet_points = v.bullet_points;

    // validate models
    const allowed = new Set(modelsCfg.map((m) => m.id));
    for (const m of models) {
      if (!allowed.has(m))
        return res.status(400).json({ error: `Unknown model: ${m}` });
    }
    if (!models.length)
      return res.status(400).json({ error: "No models selected" });

    // create job (заглушка)
    const jobId = "job_" + crypto.randomBytes(8).toString("hex");

    // тут: сохранить файл в storage, записать job в БД/очередь, запустить обработку
    res.status(201).json({ jobId, status: "queued" });
  } catch (e) {
    res.status(400).json({ error: "Bad request", details: e.message });
  }
});

app.get("/", (req, res) => res.send("OK"));

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => console.log("Listening on", PORT));
