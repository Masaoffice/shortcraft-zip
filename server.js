import express from "express";
import cors from "cors";
import archiver from "archiver";
import pLimit from "p-limit";
import { Agent, fetch } from "undici";
import { Readable } from "stream";

const app = express();
const PORT = process.env.PORT || 8080;

app.use(
  cors({
    origin: ["https://shortcraft.jp", "https://www.shortcraft.jp"],
    methods: ["POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  })
);
app.options("*", cors());
app.use(express.json({ limit: "1mb" }));

const agent = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  connectTimeout: 30_000,
  bodyTimeout: 0,
});

const sanitize = (name = "file") =>
  name.replace(/[\\/:*?"<>|]/g, "_").replace(/\s+/g, "_");

app.post("/api/create-zip", async (req, res) => {
  const { zip_name, files } = req.body || {};
  if (!Array.isArray(files) || files.length === 0) {
    return res.status(400).json({ error: "no_files" });
  }

  const zipName = sanitize(zip_name || "download.zip");
  console.log("[ZIP] start", zipName, "files:", files.length);

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);
  res.setHeader("Cache-Control", "no-store");
  res.flushHeaders();

  const archive = archiver("zip", {
    zlib: { level: 0 },
    forceZip64: true,
  });

  archive.on("warning", (e) => console.warn("[ZIP warning]", e.message));
  archive.on("error", (e) => {
    console.error("[ZIP error]", e);
    res.destroy(e);
  });

  archive.pipe(res);

  const limit = pLimit(1);

  try {
    for (let i = 0; i < files.length; i++) {
      const f = files[i];
      await limit(async () => {
        const name = sanitize(f.zip_path || `file_${i + 1}`);
        console.log("[ZIP] fetch start", name);

        const response = await fetch(f.url, { dispatcher: agent });
        if (!response.ok || !response.body) {
          throw new Error(`fetch_failed_${name}`);
        }

        // ★ ここが決定的修正
        const nodeStream = Readable.fromWeb(response.body);
        archive.append(nodeStream, { name });

        console.log("[ZIP] stream appended", name);
      });
    }

    console.log("[ZIP] finalize start");
    await archive.finalize();
    console.log("[ZIP] finalize done");
  } catch (err) {
    console.error("[ZIP fatal]", err);
    res.destroy(err);
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});
