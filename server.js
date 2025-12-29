import express from "express";
import cors from "cors";
import archiver from "archiver";
import pLimit from "p-limit";
import { Agent, fetch } from "undici";

const app = express();
const PORT = process.env.PORT || 8080;

/**
 * CORS
 * shortcraft.jp からのアクセスを明示的に許可
 */
app.use(
  cors({
    origin: [
      "https://shortcraft.jp",
      "https://www.shortcraft.jp",
    ],
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);
app.options("*", cors());
app.use(express.json({ limit: "1mb" }));

/**
 * HTTP Agent
 */
const agent = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  connectTimeout: 30_000,
  bodyTimeout: 0,
});

/**
 * util
 */
const sanitize = (name = "file") =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, "_");

/**
 * main
 */
app.post("/api/create-zip", async (req, res) => {
  const { zip_name, files } = req.body || {};

  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ error: "no_files" });
  }

  const zipName = sanitize(zip_name || "download.zip");

  res.setHeader("Content-Type", "application/zip");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${zipName}"`
  );
  res.setHeader("Cache-Control", "no-store");

  // ★ 重要：即レスポンス開始
  res.flushHeaders();

  const archive = archiver("zip", {
    zlib: { level: 0 },
    forceZip64: true,
  });

  archive.on("warning", (err) => {
    console.warn("[zip warning]", err.message);
  });

  archive.on("error", (err) => {
    console.error("[zip error]", err);
    res.destroy(err);
  });

  archive.pipe(res);

  const limit = pLimit(1); // 安定優先

  try {
    await Promise.all(
      files.map((f, idx) =>
        limit(async () => {
          const name = sanitize(f.zip_path || `file_${idx + 1}`);

          const response = await fetch(f.url, {
            dispatcher: agent,
          });

          if (!response.ok || !response.body) {
            throw new Error(`fetch_failed_${name}`);
          }

          // ★ DLしながら即ZIPへ流す（/tmp 不使用）
          archive.append(response.body, { name });
        })
      )
    );

    await archive.finalize();
  } catch (err) {
    console.error("[zip fatal]", err);
    res.destroy(err);
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});
