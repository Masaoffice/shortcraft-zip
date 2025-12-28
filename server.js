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

app.use(
  cors({
    origin: (_origin, cb) => cb(null, true),
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);
app.options("*", cors());
app.use(express.json());

const sanitize = (name) =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, " ").trim();

const agent = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  connectTimeout: 30_000,
  bodyTimeout: 0,
});

const downloadToTemp = async (url, destPath, ms = 300_000) => {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  try {
    const res = await fetch(url, { signal: controller.signal, dispatcher: agent });
    if (!res.ok || !res.body) throw new Error(`fetch_failed_${res.status}`);
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
  const limit = pLimit(1);

  const zipId = randomUUID();
  const zipPath = path.join(TEMP_DIR, `${zipId}.zip`);

  console.log(`[zip] build start name=${zipName} files=${files.length}`);

  const archive = archiver("zip", {
    zlib: { level: 0 },
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
          const zipEntryName = sanitize(f.zip_path || `video_${idx + 1}.bin`);
          const tempFile = path.join(
            TEMP_DIR,
            `src_${Date.now()}_${idx}_${Math.random().toString(36).slice(2)}.tmp`
          );
          tempFiles.push(tempFile);

          const maxRetry = 3;
          for (let attempt = 1; attempt <= maxRetry; attempt++) {
            try {
              console.log(`[zip] download start ${zipEntryName} attempt=${attempt}`);
              await downloadToTemp(f.url, tempFile);
              archive.file(tempFile, { name: zipEntryName });
              console.log(`[zip] registered ${zipEntryName}`);
              return;
            } catch (e) {
              console.warn(`[zip] download error ${zipEntryName} attempt=${attempt}`, e.message);
              if (attempt === maxRetry) {
                failed.push({ zip_path: zipEntryName, reason: e.message });
                if (!allowPartial) throw e;
                archive.append(
                  `Error downloading ${zipEntryName}: ${e.message}`,
                  { name: `${zipEntryName}.error.txt` }
                );
              } else {
                await new Promise((r) => setTimeout(r, 2000 * attempt));
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
      `[zip] build done status=${failed.length ? "partial_failed" : "completed"} size=${stat.size}`
    );

    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);
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
