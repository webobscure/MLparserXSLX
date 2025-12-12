const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");

const app = express();
app.set("trust proxy", 1);
app.use(cors());

// Загружаем файл в память (без записи на диск)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 15 * 1024 * 1024 }, // 15MB
  fileFilter: (req, file, cb) => {
    const ok =
      file.mimetype ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      file.mimetype === "application/vnd.ms-excel" || // иногда прилетает так
      file.originalname.toLowerCase().endsWith(".xlsx") ||
      file.originalname.toLowerCase().endsWith(".xls");
    cb(ok ? null : new Error("Only Excel files (.xlsx/.xls) are allowed"), ok);
  },
});

// Утилита: находим первую “не пустую” строку как заголовки
function findHeaderRowAoA(aoa) {
  for (let r = 0; r < aoa.length; r++) {
    const row = aoa[r] || [];
    const hasAny = row.some((v) => v !== null && v !== undefined && String(v).trim() !== "");
    if (hasAny) return r;
  }
  return 0;
}

// Утилита: Excel-колонка A, B, C... AA...
function colLetter(n) {
  let s = "";
  n += 1;
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

const swaggerUi = require("swagger-ui-express");
const swaggerJSDoc = require("swagger-jsdoc");

const swaggerSpec = swaggerJSDoc({
  definition: {
    openapi: "3.0.0",
    info: { title: "Excel Mapper API", version: "1.0.0" },
    servers: [{ url: "/" }], // ✅ важно
  },
  apis: [],
});

// Минимально можно описать руками один эндпоинт:
swaggerSpec.paths = {
  "/api/parse-excel": {
    post: {
      summary: "Upload Excel and get columns + preview",
      parameters: [
        { name: "sheet", in: "query", schema: { type: "string" } },
        { name: "headerRow", in: "query", schema: { type: "integer" } },
        { name: "previewRows", in: "query", schema: { type: "integer", default: 5 } },
      ],
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
        200: { description: "Parsed info" },
      },
    },
  },
};

app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));
app.get("/", (req, res) => res.send("OK"));

/**
 * POST /api/parse-excel
 * form-data:
 *  - file: Excel
 * optional query:
 *  - sheet: имя листа
 *  - headerRow: номер строки заголовков (1-based). если не указан — авто-поиск
 *  - previewRows: сколько строк превью вернуть (по умолчанию 5)
 */
app.post("/api/parse-excel", upload.single("file"), (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded (field name must be 'file')" });
    }

    const previewRows = Math.max(0, Math.min(50, Number(req.query.previewRows ?? 5)));

    const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
    const sheetNames = workbook.SheetNames;

    if (!sheetNames.length) {
      return res.status(400).json({ error: "Workbook has no sheets" });
    }

    const requestedSheet = req.query.sheet ? String(req.query.sheet) : null;
    const sheetName = requestedSheet && sheetNames.includes(requestedSheet)
      ? requestedSheet
      : sheetNames[0];

    const sheet = workbook.Sheets[sheetName];

    // Получаем двумерный массив всех ячеек (как таблицу)
    const aoa = XLSX.utils.sheet_to_json(sheet, {
      header: 1,       // array-of-arrays
      raw: false,      // строки/даты в более читаемом виде
      defval: "",      // пустые ячейки -> ""
      blankrows: false,
    });

    // headerRow: 1-based из query или авто
    let headerRowIndex;
    if (req.query.headerRow) {
      const hr = Number(req.query.headerRow);
      headerRowIndex = Number.isFinite(hr) && hr > 0 ? hr - 1 : 0;
    } else {
      headerRowIndex = findHeaderRowAoA(aoa);
    }

    const headerRow = aoa[headerRowIndex] || [];

    // Определяем “ширину” таблицы (максимум столбцов среди первых строк)
    const width = Math.max(
      headerRow.length,
      ...aoa.slice(headerRowIndex, headerRowIndex + 30).map((r) => (r ? r.length : 0)),
      0
    );

    // Собираем список колонок
    const columns = Array.from({ length: width }, (_, idx) => {
      const name = headerRow[idx] !== undefined && String(headerRow[idx]).trim() !== ""
        ? String(headerRow[idx]).trim()
        : `Column ${colLetter(idx)}`;

      return {
        index: idx,              // 0-based
        letter: colLetter(idx),  // A, B, C...
        header: name,
      };
    });

    // Превью строк: несколько строк после заголовка
    const startDataRow = headerRowIndex + 1;
    const preview = aoa
      .slice(startDataRow, startDataRow + previewRows)
      .map((row) => {
        const obj = {};
        for (let i = 0; i < width; i++) {
          obj[columns[i].header] = row?.[i] ?? "";
        }
        return obj;
      });

    return res.json({
      fileName: req.file.originalname,
      sheets: sheetNames,
      selectedSheet: sheetName,
      headerRow: headerRowIndex + 1, // вернем 1-based
      columns,
      preview,
    });
  } catch (e) {
    return res.status(500).json({ error: e.message || "Parse failed" });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Excel parser server running on port ${PORT}`);
});

process.on("uncaughtException", (e) => console.error("uncaughtException:", e));
process.on("unhandledRejection", (e) => console.error("unhandledRejection:", e));