import express from "express";
import archiver from "archiver";
import cors from "cors";
import { Agent } from "undici";
import pLimit from "p-limit";
import fs from "fs";
import path from "path";
import { pipeline } from "stream/promises";

const app = express();
const PORT = process.env.PORT || 8080;
const TEMP_DIR = "/tmp";

/**
 * ===== CORS（本番用・正解実装）=====
 * ・Error を投げない
 * ・許可 origin のみ true
 */
app.use(
  cors({
    origin: (origin, cb) => {
      // server-to-server / curl / health check
      if (!origin) return cb(null, true);

      if (
        origin === "https://shortcraft.jp" ||
        origin === "https://www.shortcraft.jp"
      ) {
        return cb(null, true);
      }

      // ❗ Error を投げないのが重要
      return cb(null, false);
    },
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);

app.options("*", cors());
app.use(express.json({ limit: "1mb" }));

const sanitize = (name = "") =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, " ").trim();

/**
 * undici Agent（長時間DL前提）
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
      dispatcher: agent,
      signal: controller.signal,
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

/**
 * ===== ZIP API =====
 */
app.post("/api/create-zip", async (req, res) => {
  const { zip_name, files, options } = req.body || {};

  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ status: "failed", reason: "no_files" });
  }

  const zipName = sanitize(zip_name || "download.zip");
  const allowPartial = options?.allowPartial !== false;

  const limit = pLimit(1); // 並列1固定
  console.log(`[zip] start name=${zipName} files=${files.length}`);

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);

  const archive = archiver("zip", {
    zlib: { level: 0 },
    forceZip64: true,
  });

  const tempFiles = [];
  let isAborted = false;

  archive.on("warning", (err) =>
    console.warn("[zip] warning:", err.message)
  );

  archive.on("error", (err) => {
    console.error("[zip] fatal error:", err);
    if (!res.headersSent) res.status(500).end();
    else res.destroy(err);
  });

  res.on("close", () => {
    if (!res.writableEnded) {
      console.warn("[zip] aborted by client");
      isAborted = true;
      archive.abort();
    }
  });

  archive.pipe(res);

  try {
    await Promise.all(
      files.map((f, idx) =>
        limit(async () => {
          if (isAborted) return;

          const zipPath = sanitize(f.zip_path || `file_${idx + 1}`);
          const tempFile = path.join(
            TEMP_DIR,
            `tmp_${Date.now()}_${idx}_${Math.random()
              .toString(36)
              .slice(2)}`
          );

          tempFiles.push(tempFile);
          const maxRetry = 3;

          for (let attempt = 1; attempt <= maxRetry; attempt++) {
            if (isAborted) break;

            try {
              console.log(
                `[zip] download start: ${zipPath} attempt=${attempt}`
              );
              await downloadToTemp(f.url, tempFile);
              archive.file(tempFile, { name: zipPath });
              console.log(`[zip] registered: ${zipPath}`);
              return;
            } catch (e) {
              console.warn(
                `[zip] download error: ${zipPath} attempt=${attempt}`,
                e.message
              );

              if (attempt === maxRetry) {
                if (!allowPartial) throw e;
                archive.append(
                  `Failed to download ${zipPath}: ${e.message}`,
                  { name: `${zipPath}.error.txt` }
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
    console.log("[zip] completed");
  } catch (err) {
    console.error("[zip] process failed:", err);
    res.destroy(err);
  } finally {
    console.log(`[zip] cleanup ${tempFiles.length} files`);
    await Promise.all(
      tempFiles.map(async (p) => {
        try {
          await fs.promises.unlink(p);
        } catch {}
      })
    );
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});
