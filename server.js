import express from "express";
import cors from "cors";
import archiver from "archiver";
import pLimit from "p-limit";
import { Agent, fetch } from "undici";
import { PassThrough } from "stream";
import { finished } from "stream/promises";

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

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);
  res.setHeader("Cache-Control", "no-store");
  res.flushHeaders();

  const archive = archiver("zip", {
    zlib: { level: 0 },
    forceZip64: true,
  });

  archive.on("error", (err) => {
    console.error("[zip error]", err);
    res.destroy(err);
  });

  archive.pipe(res);

  const limit = pLimit(1);

  try {
    for (let i = 0; i < files.length; i++) {
      const f = files[i];
      await limit(async () => {
        const name = sanitize(f.zip_path || `file_${i + 1}`);

        const response = await fetch(f.url, { dispatcher: agent });
        if (!response.ok || !response.body) {
          throw new Error(`fetch_failed_${name}`);
        }

        // ★ 重要：中継ストリーム
        const pass = new PassThrough();
        archive.append(pass, { name });

        response.body.pipe(pass);
        await finished(pass); // ★ 完全に流れ切るまで待つ
      });
    }

    await archive.finalize(); // ★ 全部終わってから
  } catch (err) {
    console.error("[zip fatal]", err);
    res.destroy(err);
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});
