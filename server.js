import express from "express";
import cors from "cors";
import archiver from "archiver";
import pLimit from "p-limit";
import { Readable } from "stream";

const app = express();
const PORT = process.env.PORT || 8080;

// ===== CORS =====
app.use(
  cors({
    origin: (_origin, cb) => cb(null, true),
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);
app.options("*", cors());
app.use(express.json());

// ===== Utils =====
const sanitize = (name) =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, " ").trim();

const fetchWithTimeout = async (url, ms = 120_000) => {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(t);
  }
};

// ===== API =====
app.post("/api/create-zip", async (req, res) => {
  const { zip_name, files, options } = req.body || {};
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ status: "failed", reason: "no_files" });
  }

  const zipName = sanitize(zip_name || "download.zip");
  const allowPartial = options?.allowPartial !== false;
  const parallel = Math.max(1, Math.min(options?.parallel || 4, 6));

  console.log(`[zip] start name=${zipName} files=${files.length} parallel=${parallel}`);

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);

  const archive = archiver("zip", { zlib: { level: 6 } });
  const limit = pLimit(parallel);
  const failed = [];

  archive.on("warning", (err) => {
    console.warn("[zip] warning:", err.message);
  });

  archive.on("error", (err) => {
    console.error("[zip] fatal:", err);
    if (!res.headersSent) res.status(500);
    res.end();
  });

  archive.pipe(res);

  res.on("close", () => {
    if (!res.writableEnded) {
      console.warn("[zip] aborted by client");
      try {
        archive.abort();
      } catch {}
    }
  });

  await Promise.all(
    files.map((f, idx) =>
      limit(async () => {
        const zipPath = sanitize(f.zip_path || `video_${idx + 1}.bin`);
        try {
          console.log(`[zip] file start: ${zipPath}`);
          const r = await fetchWithTimeout(f.url);
          if (!r.ok || !r.body) {
            throw new Error(`fetch_failed_${r.status}`);
          }

          // ★ 正解：Web Stream → Node Stream
          const nodeStream = Readable.fromWeb(r.body);
          archive.append(nodeStream, { name: zipPath });
        } catch (e) {
          console.warn(`[zip] file error: ${zipPath}`, e.message);
          failed.push({ zip_path: zipPath, reason: e.message });
          if (!allowPartial) throw e;
        }
      })
    )
  );

  await archive.finalize();
  console.log("[zip] finalize");
  console.log(
    `[zip] done status=${failed.length ? "partial_failed" : "completed"} failed=${failed.length}`
  );
});

// ===== Health =====
app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on :${PORT}`);
});
