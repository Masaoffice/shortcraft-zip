import express from "express";
import archiver from "archiver";
import cors from "cors";
import { v4 as uuidv4 } from "uuid";
import fs from "fs";
import path from "path";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const STORAGE_BUCKET_VIDEOS = process.env.STORAGE_BUCKET_VIDEOS || "videos";
const STORAGE_BUCKET_TEMP = process.env.STORAGE_BUCKET_TEMP || "temp";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const app = express();

app.use(
  cors({
    origin: (origin, callback) => {
      // localhost / 本番 / Cloud Run すべて許可
      callback(null, true);
    },
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type"],
  }),
);

// preflight を明示的に許可
app.options("*", cors());

app.use(express.json());

// 簡易ジョブ管理（本番はDB/Redis推奨）
const jobs = new Map(); // jobId -> { status, url, error }

app.post("/api/create-zip", async (req, res) => {
  const { videos, zipName } = req.body;
  if (!videos || !Array.isArray(videos) || videos.length === 0) {
    return res.status(400).json({ error: "Invalid videos list" });
  }
  const jobId = uuidv4();
  jobs.set(jobId, { status: "processing" });
  processZipJob(jobId, videos, zipName).catch((err) => {
    console.error(`Job ${jobId} failed:`, err);
    jobs.set(jobId, { status: "failed", error: err.message });
  });
  res.json({ jobId });
});

app.get("/api/zip-status/:jobId", (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: "Job not found" });
  res.json(job);
});

async function processZipJob(jobId, videos, zipName = "download.zip") {
  const fileName = `zip_${jobId}.zip`;
  const tmpPath = path.join("/tmp", fileName);
  const output = fs.createWriteStream(tmpPath);
  const archive = archiver("zip", { zlib: { level: 5 } });

  const finalizePromise = new Promise((resolve, reject) => {
    output.on("close", resolve);
    archive.on("error", reject);
  });

  archive.pipe(output);

  for (const video of videos) {
    const name = video.fileName || path.basename(video.storagePath);
    const maxRetry = 2;
    let success = false;

    for (let attempt = 1; attempt <= maxRetry; attempt += 1) {
      try {
        const { data, error } = await supabase.storage
          .from(STORAGE_BUCKET_VIDEOS)
          .download(video.storagePath);
        if (error) throw error;

        const buffer = Buffer.from(await data.arrayBuffer());
        archive.append(buffer, { name });
        success = true;
        break;
      } catch (err) {
        if (attempt === maxRetry) {
          archive.append(
            `Error processing: ${video.storagePath}\n${err.message}`,
            { name: `${name}.error.txt` },
          );
        } else {
          // 少し待ってリトライ
          await new Promise((res) => setTimeout(res, 500));
        }
      }
    }

    if (!success) {
      // 失敗した場合も処理続行（部分成功を許容）
      continue;
    }
  }

  await archive.finalize();
  await finalizePromise;

  try {
    const storagePath = `zips/${fileName}`;
    const fileBuffer = fs.readFileSync(tmpPath);
    const { error: uploadError } = await supabase.storage
      .from(STORAGE_BUCKET_TEMP)
      .upload(storagePath, fileBuffer, {
        contentType: "application/zip",
        upsert: true,
      });
    if (uploadError) throw uploadError;

    const { data: signed, error: signError } = await supabase.storage
      .from(STORAGE_BUCKET_TEMP)
      .createSignedUrl(storagePath, 3600);
    if (signError) throw signError;

    jobs.set(jobId, {
      status: "completed",
      url: signed.signedUrl,
      name: zipName,
    });
  } catch (err) {
    jobs.set(jobId, { status: "failed", error: err.message });
  } finally {
    if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
  }
}

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`ZIP service running on port ${PORT}`);
});

