import express from "express";
import archiver from "archiver";
import cors from "cors";
import { Agent } from "undici";
import pLimit from "p-limit";
import fs from "fs";
import path from "path";
import { pipeline } from "stream/promises";
import { randomUUID } from "crypto";

const app = express();
const PORT = process.env.PORT || 8080;
const TEMP_DIR = "/tmp";

/**
 * ===== CORS 設定（本番対応）=====
 * 明示的に shortcraft.jp を許可
 */
const ALLOWED_ORIGINS = [
  "https://shortcraft.jp",
  "https://www.shortcraft.jp",
];

app.use(
  cors({
    origin: (origin, cb) => {
      // サーバー間通信 / curl / health check
      if (!origin) return cb(null, true);

      if (ALLOWED_ORIGINS.includes(origin)) {
        return cb(null, true);
      }

      return cb(new Error(`CORS blocked: ${origin}`), false);
    },
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.options("*", cors());
app.use(express.json());

const sanitize = (name) =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, " ").trim();

/**
 * ===== HTTP Agent =====
 * 大容量ダウンロード安定化
 */
const agent = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  connectTimeout: 30_000,
  bodyTimeout: 0, // Cloud Run timeout に委ねる
});

const downloadToTemp = async (url, destPath, ms = 300_000) => {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, {
      signal: controller.signal,
      dispatcher: agent,
    });
    if (!res.ok || !res.body) {
      throw new Error(`fetch_failed_${res.status}`);
    }
    const fileStream = fs.createWriteStream(destPath);
    await pipeline(res.body, fileStream);
  } finally {
    clearTimeout(t);
  }
};

app.post("/api/create-zip", async (req, res) => {
  const { zip_name, files, options } = req.body || {};
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ status: "failed", reason: "no_files" });
  }

  const zipName = sanitize(zip_name || "download.zip");
  const allowPartial = options?.allowPartial !== false;

  // 並列は 1 固定（安定性優先）
  const limit = pLimit(1);

  const zipId = randomUUID();
  const zipPath = path.join(TEMP_DIR, `${zipId}.zip`);

  console.log(`[zip] build start name=${zipName} files=${files.length}`);

  const archive = archiver("zip", {
    zlib: { level: 0 }, // 圧縮なし（動画向け）
    forceZip64: true,
  });

  const output = fs.createWriteStream(zipPath);
  archive.pipe(output);

  const tempFiles = [];
  const failed = [];

  try {
    await Promise.all(
      files.map((f, idx) =>
        limit(async () => {
          const entryName = sanitize(f.zip_path || `video_${idx + 1}.bin`);
          const tempFile = path.join(
            TEMP_DIR,
            `src_${Date.now()}_${idx}_${Math.random().toString(36).slice(2)}.tmp`
          );
          tempFiles.push(tempFile);

          const maxRetry = 3;
          for (let attempt = 1; attempt <= maxRetry; attempt++) {
            try {
              console.log(
                `[zip] download start ${entryName} attempt=${attempt}`
              );
              await downloadToTemp(f.url, tempFile);
              archive.file(tempFile, { name: entryName });
              console.log(`[zip] registered ${entryName}`);
              return;
            } catch (e) {
              console.warn(
                `[zip] download error ${entryName} attempt=${attempt}`,
                e.message
              );
              if (attempt === maxRetry) {
                failed.push({ zip_path: entryName, reason: e.message });
                if (!allowPartial) throw e;
                archive.append(
                  `Error downloading ${entryName}: ${e.message}`,
                  { name: `${entryName}.error.txt` }
                );
              } else {
                await new Promise((r) =>
                  setTimeout(r, 2000 * attempt)
                );
              }
            }
          }
        })
      )
    );

    await archive.finalize();
    await new Promise((r) => output.on("close", r));

    const stat = await fs.promises.stat(zipPath);

    console.log(
      `[zip] build done status=${
        failed.length ? "partial_failed" : "completed"
      } size=${stat.size}`
    );

    res.setHeader("Content-Type", "application/zip");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="${zipName}"`
    );
    res.setHeader("Content-Length", stat.size);

    const zipStream = fs.createReadStream(zipPath);
    zipStream.pipe(res);

    res.on("close", async () => {
      try {
        await fs.promises.unlink(zipPath);
      } catch {}
    });
  } catch (err) {
    console.error("[zip] fatal error:", err);
    res.status(500).end();
    try {
      await fs.promises.unlink(zipPath);
    } catch {}
  } finally {
    for (const p of tempFiles) {
      try {
        await fs.promises.unlink(p);
      } catch {}
    }
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});
