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
  bodyTimeout: 0, // 無制限（Cloud Run側でTimeout=3600sを設定すること）
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
  
  // 並列数は1固定（大容量ファイルのメモリ圧迫と帯域競合回避のため）
  const limit = pLimit(1); 

  console.log(`[zip] start name=${zipName} files=${files.length} parallel=1`);

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="${zipName}"`);

  // 【重要】6GB超え対応のため forceZip64: true を追加
  // 動画ファイルなので圧縮なし(STORE)でCPU負荷を軽減
  const archive = archiver("zip", { 
    zlib: { level: 0 },
    forceZip64: true 
  });
  
  const failed = [];
  const tempFiles = []; // 作成した一時ファイルを追跡
  let isAborted = false;

  archive.on("warning", (err) => console.warn("[zip] warning:", err.message));
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

          const zipPath = sanitize(f.zip_path || `video_${idx + 1}.bin`);
          const tempFileName = `temp_${Date.now()}_${idx}_${Math.random().toString(36).slice(2)}.tmp`;
          const tempFilePath = path.join(TEMP_DIR, tempFileName);
          
          // 後で削除するために記録
          tempFiles.push(tempFilePath);

          const maxRetry = 3;
          for (let attempt = 1; attempt <= maxRetry; attempt += 1) {
            if (isAborted) break;
            try {
              const start = Date.now();
              console.log(`[zip] download start: ${zipPath} attempt=${attempt}/${maxRetry}`);
              
              // 1. 一時ファイルへダウンロード
              await downloadToTemp(f.url, tempFilePath);
              
              // 2. 成功したらZIPへ登録
              // finalize() が呼ばれるまで実際には読み込まれない可能性があるため、
              // ここではファイルパスを渡すだけにしておく
              archive.file(tempFilePath, { name: zipPath });
              
              const elapsed = Date.now() - start;
              console.log(`[zip] registered: ${zipPath} elapsed=${elapsed}ms`);
              return; 

            } catch (e) {
              console.warn(`[zip] download error: ${zipPath} attempt=${attempt}`, e.message);
              
              // 失敗した場合、最後の試行でなければリトライ
              if (attempt === maxRetry) {
                failed.push({ zip_path: zipPath, reason: e.message });
                if (!allowPartial) throw e; 
                // Partial許可ならエラーテキストを追加
                archive.append(`Error downloading ${zipPath}: ${e.message}`, { name: `${zipPath}.error.txt` });
              } else {
                const waitMs = 2000 * Math.pow(2, attempt - 1);
                await new Promise((res) => setTimeout(res, waitMs));
              }
            }
          }
        })
      )
    );

    // 全ファイルの登録完了を待ってfinalize
    await archive.finalize();
    console.log(`[zip] done status=${failed.length ? "partial_failed" : "completed"} failed=${failed.length}`);

  } catch (err) {
    console.error("[zip] process failed:", err);
    // クライアントへネットワークエラーとして通知
    res.destroy(err);
  } finally {
    // クリーンアップ: 記録された一時ファイルを全て削除
    console.log(`[zip] cleaning up ${tempFiles.length} temp files...`);
    await Promise.all(
      tempFiles.map(async (p) => {
        try {
          await fs.promises.unlink(p);
        } catch (e) {
          // 無視
        }
      })
    );
  }
});

app.get("/health", (_, res) => res.status(200).send("ok"));

app.listen(PORT, () => {
  console.log(`ZIP service listening on port ${PORT}`);
});

// test trigger