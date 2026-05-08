const express = require("express");
const multer = require("multer");
const cors = require("cors");
const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const AWS = require("aws-sdk");
const { Pool } = require("pg");
const https = require("https");
const crypto = require("crypto");

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const pool = new Pool({
  user: "postgres",
  host: "34.93.25.211",
  database: "anoxx",
  password: process.env.DB_PASSWORD,
  port: 5432,
  ssl: {
    rejectUnauthorized: false,
  },
});

const uploadDir = "/tmp/uploads";
const outputDir = "/tmp/output";
const hlsDir = "/tmp/hls";

fs.mkdirSync(uploadDir, { recursive: true });
fs.mkdirSync(outputDir, { recursive: true });
fs.mkdirSync(hlsDir, { recursive: true });

const upload = multer({ dest: uploadDir });

const requiredEnv = [
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET_NAME",
  "R2_PUBLIC_URL",
  "DB_PASSWORD",
];

for (const key of requiredEnv) {
  if (!process.env[key]) {
    console.error(`Missing env variable: ${key}`);
  }
}

const s3 = new AWS.S3({
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  accessKeyId: process.env.R2_ACCESS_KEY_ID,
  secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  region: "auto",
  signatureVersion: "v4",
  s3ForcePathStyle: true,
});

const HLS_QUALITIES = [
  {
    name: "144p",
    height: 144,
    bandwidth: 180000,
    videoBitrate: "150k",
    audioBitrate: "48k",
  },
  {
    name: "240p",
    height: 240,
    bandwidth: 350000,
    videoBitrate: "300k",
    audioBitrate: "64k",
  },
  {
    name: "360p",
    height: 360,
    bandwidth: 700000,
    videoBitrate: "600k",
    audioBitrate: "96k",
  },
  {
    name: "480p",
    height: 480,
    bandwidth: 1200000,
    videoBitrate: "1000k",
    audioBitrate: "128k",
  },
  {
    name: "720p",
    height: 720,
    bandwidth: 2500000,
    videoBitrate: "2200k",
    audioBitrate: "128k",
  },
  {
    name: "1080p",
    height: 1080,
    bandwidth: 5000000,
    videoBitrate: "4500k",
    audioBitrate: "192k",
  },
];

const WORKER_ID = `${process.env.K_SERVICE || "local"}-${
  process.env.K_REVISION || "dev"
}-${crypto.randomUUID()}`;

const AUTO_WORKER_ENABLED = process.env.AUTO_WORKER_ENABLED !== "false";
const AUTO_WORKER_INTERVAL_MS = Number(
  process.env.AUTO_WORKER_INTERVAL_MS || 30000
);
const PROCESSING_STALE_MINUTES = Number(
  process.env.PROCESSING_STALE_MINUTES || 120
);

function cleanFileName(name) {
  return String(name || "file")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 80);
}

function getFileExtension(fileName, fallback) {
  const ext = path.extname(fileName || "").toLowerCase();
  return ext || fallback;
}

async function getCreatorUser(firebaseUid) {
  const result = await pool.query(
    "SELECT * FROM users WHERE firebase_uid = $1",
    [firebaseUid]
  );

  return result.rows[0] || null;
}

async function requireCreator(firebaseUid) {
  if (!firebaseUid) {
    const error = new Error("firebase_uid is required");
    error.status = 400;
    throw error;
  }

  const user = await getCreatorUser(firebaseUid);

  if (!user) {
    const error = new Error("User not found. Please sync user first.");
    error.status = 404;
    throw error;
  }

  if (user.is_creator !== true) {
    const error = new Error("Creator access required");
    error.status = 403;
    throw error;
  }

  return user;
}

function downloadFile(url, outputPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(outputPath);

    https
      .get(url, (response) => {
        if (response.statusCode !== 200) {
          file.close(() => {});
          fs.unlink(outputPath, () => {});
          reject(new Error(`Download failed: ${response.statusCode}`));
          return;
        }

        response.pipe(file);

        file.on("finish", () => {
          file.close(resolve);
        });
      })
      .on("error", (err) => {
        file.close(() => {});
        fs.unlink(outputPath, () => {});
        reject(err);
      });
  });
}

function runFFmpeg(command) {
  return new Promise((resolve, reject) => {
    exec(command, { maxBuffer: 1024 * 1024 * 20 }, (error, stdout, stderr) => {
      if (error) {
        reject(new Error(stderr || error.message || "FFmpeg failed"));
        return;
      }

      resolve({ stdout, stderr });
    });
  });
}

function getContentTypeForHls(filePath) {
  if (filePath.endsWith(".m3u8")) {
    return "application/vnd.apple.mpegurl";
  }

  if (filePath.endsWith(".ts")) {
    return "video/mp2t";
  }

  return "application/octet-stream";
}

function listFilesRecursive(dir) {
  const results = [];

  for (const item of fs.readdirSync(dir)) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      results.push(...listFilesRecursive(fullPath));
    } else {
      results.push(fullPath);
    }
  }

  return results;
}

function deleteFolderSafe(folderPath) {
  if (folderPath && fs.existsSync(folderPath)) {
    fs.rmSync(folderPath, { recursive: true, force: true });
  }
}

function createHlsMasterPlaylist(qualities) {
  const lines = ["#EXTM3U", "#EXT-X-VERSION:3"];

  for (const quality of qualities) {
    lines.push(
      `#EXT-X-STREAM-INF:BANDWIDTH=${quality.bandwidth},RESOLUTION=${quality.width}x${quality.height}`
    );
    lines.push(`${quality.name}/index.m3u8`);
  }

  return `${lines.join("\n")}\n`;
}

async function uploadHlsFolder(localFolder, r2BaseKey) {
  const files = listFilesRecursive(localFolder);
  let totalBytes = 0;

  for (const filePath of files) {
    const relativePath = path.relative(localFolder, filePath).replace(/\\/g, "/");
    const key = `${r2BaseKey}/${relativePath}`;
    const stats = fs.statSync(filePath);
    totalBytes += stats.size;

    await s3
      .upload({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: key,
        Body: fs.createReadStream(filePath),
        ContentType: getContentTypeForHls(filePath),
      })
      .promise();
  }

  return {
    fileCount: files.length,
    totalBytes,
  };
}

async function processVideo(video) {
  const processingInput = `/tmp/${crypto.randomUUID()}-input.mp4`;
  const hlsJobId = `${video.id}-${Date.now()}-${crypto.randomUUID()}`;
  const localHlsOutputDir = path.join(hlsDir, hlsJobId);

  try {
    console.log(`Starting HLS processing for video ${video.id}`);

    await pool.query(
      `
      UPDATE videos
      SET processing_status = 'processing',
          processing_error = NULL,
          processing_started_at = COALESCE(processing_started_at, CURRENT_TIMESTAMP),
          processing_worker_id = COALESCE(processing_worker_id, $2)
      WHERE id = $1
      `,
      [video.id, video.processing_worker_id || WORKER_ID]
    );

    fs.mkdirSync(localHlsOutputDir, { recursive: true });

    await downloadFile(video.original_video_url, processingInput);

    const completedQualities = [];

    for (const quality of HLS_QUALITIES) {
      const qualityDir = path.join(localHlsOutputDir, quality.name);
      fs.mkdirSync(qualityDir, { recursive: true });

      const segmentPattern = path.join(qualityDir, "segment_%03d.ts");
      const playlistPath = path.join(qualityDir, "index.m3u8");

      const ffmpegCommand = `ffmpeg -y -i "${processingInput}" -vf "scale=-2:${quality.height}" -c:v libx264 -preset medium -b:v ${quality.videoBitrate} -maxrate ${quality.videoBitrate} -bufsize ${quality.videoBitrate} -c:a aac -b:a ${quality.audioBitrate} -hls_time 6 -hls_playlist_type vod -hls_segment_filename "${segmentPattern}" "${playlistPath}"`;

      console.log(`Generating ${quality.name} HLS for video ${video.id}`);
      await runFFmpeg(ffmpegCommand);

      completedQualities.push({
        ...quality,
        width: Math.round((quality.height * 16) / 9),
      });
    }

    const masterPlaylist = createHlsMasterPlaylist(completedQualities);
    const masterPath = path.join(localHlsOutputDir, "master.m3u8");
    fs.writeFileSync(masterPath, masterPlaylist);

    const creatorUid = video.creator_uid || "unknown";
    const hlsBaseKey = `videos/hls/${creatorUid}/${video.id}/${Date.now()}`;
    const masterKey = `${hlsBaseKey}/master.m3u8`;
    const masterUrl = `${process.env.R2_PUBLIC_URL}/${masterKey}`;

    const uploadStats = await uploadHlsFolder(localHlsOutputDir, hlsBaseKey);

    await pool.query(
      `
      UPDATE videos
      SET
        processing_status = 'ready',
        hls_master_url = $1,
        hls_master_key = $2,
        available_qualities = $3,
        processed_video_url = $1,
        processed_video_key = $2,
        video_url = $1,
        video_key = $2,
        file_size_bytes = $4,
        processing_error = NULL,
        processing_started_at = NULL,
        processing_worker_id = NULL
      WHERE id = $5
      `,
      [
        masterUrl,
        masterKey,
        completedQualities.map((quality) => quality.name),
        uploadStats.totalBytes,
        video.id,
      ]
    );

    console.log(
      `HLS processing complete for video ${video.id}. Uploaded ${uploadStats.fileCount} files.`
    );
  } catch (error) {
    console.error("HLS PROCESSING ERROR:", error);

    await pool.query(
      `
      UPDATE videos
      SET
        processing_status = 'failed',
        processing_error = $1,
        processing_started_at = NULL,
        processing_worker_id = NULL
      WHERE id = $2
      `,
      [error.message, video.id]
    );
  } finally {
    if (fs.existsSync(processingInput)) {
      fs.unlinkSync(processingInput);
    }

    deleteFolderSafe(localHlsOutputDir);
  }
}

function handleRouteError(res, label, err) {
  console.error(label, err);

  return res.status(err.status || 500).json({
    ok: false,
    error: err.message || "Server error",
    details: err.status ? undefined : err.message,
  });
}

async function claimNextQueuedVideo() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const claimResult = await client.query(
      `
      WITH candidate AS (
        SELECT id
        FROM videos
        WHERE
          processing_status = 'queued'
          OR (
            processing_status = 'processing'
            AND processing_started_at IS NOT NULL
            AND processing_started_at <
              CURRENT_TIMESTAMP - ($1::int * INTERVAL '1 minute')
          )
        ORDER BY id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
      )
      UPDATE videos AS v
      SET
        processing_status = 'processing',
        processing_started_at = CURRENT_TIMESTAMP,
        processing_worker_id = $2,
        processing_error = NULL
      FROM candidate
      WHERE v.id = candidate.id
      RETURNING v.*
      `,
      [PROCESSING_STALE_MINUTES, WORKER_ID]
    );

    await client.query("COMMIT");

    return claimResult.rows[0] || null;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

app.get("/", (req, res) => {
  res.send("ANOXX backend is running");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    r2Account: !!process.env.R2_ACCOUNT_ID,
    r2Key: !!process.env.R2_ACCESS_KEY_ID,
    r2Secret: !!process.env.R2_SECRET_ACCESS_KEY,
    r2Bucket: !!process.env.R2_BUCKET_NAME,
    r2PublicUrl: !!process.env.R2_PUBLIC_URL,
    dbPassword: !!process.env.DB_PASSWORD,
  });
});

app.get("/test-db", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({
      ok: true,
      time: result.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "DB ERROR:", err);
  }
});

app.get("/videos", async (req, res) => {
  try {
    const result = await pool.query("SELECT * FROM videos ORDER BY id DESC");

    res.json({
      ok: true,
      videos: result.rows,
    });
  } catch (err) {
    return handleRouteError(res, "VIDEOS FETCH ERROR:", err);
  }
});

app.post("/videos/:id/process", async (req, res) => {
  try {
    const videoId = Number(req.params.id);

    if (!Number.isInteger(videoId) || videoId < 1) {
      return res.status(400).json({
        ok: false,
        error: "Valid video id is required",
      });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM videos
      WHERE id = $1
      LIMIT 1
      `,
      [videoId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "Video not found",
      });
    }

    const video = result.rows[0];

    if (!video.original_video_url) {
      return res.status(400).json({
        ok: false,
        error: "original_video_url missing",
      });
    }

    await processVideo(video);

    const updatedResult = await pool.query(
      `
      SELECT *
      FROM videos
      WHERE id = $1
      LIMIT 1
      `,
      [videoId]
    );

    return res.json({
      ok: true,
      message: "Video HLS processing finished",
      video: updatedResult.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "VIDEO PROCESS ERROR:", err);
  }
});

app.post("/processing/process-next", async (req, res) => {
  try {
    const video = await claimNextQueuedVideo();

    if (!video) {
      return res.json({
        ok: true,
        message: "No queued videos found",
      });
    }

    if (!video.original_video_url) {
      await pool.query(
        `
        UPDATE videos
        SET processing_status = 'failed',
            processing_error = 'original_video_url missing',
            processing_started_at = NULL,
            processing_worker_id = NULL
        WHERE id = $1
        `,
        [video.id]
      );

      return res.status(400).json({
        ok: false,
        error: "original_video_url missing",
        video_id: video.id,
      });
    }

    await processVideo(video);

    const updatedResult = await pool.query(
      `
      SELECT *
      FROM videos
      WHERE id = $1
      LIMIT 1
      `,
      [video.id]
    );

    return res.json({
      ok: true,
      message: "Queued video HLS processed",
      video: updatedResult.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "PROCESS NEXT ERROR:", err);
  }
});

app.post("/users/sync", async (req, res) => {
  try {
    const { firebase_uid, name, email } = req.body;

    if (!firebase_uid || !email) {
      return res.status(400).json({
        ok: false,
        error: "firebase_uid and email are required",
      });
    }

    const result = await pool.query(
      `
      INSERT INTO users (firebase_uid, name, email)
      VALUES ($1, $2, $3)
      ON CONFLICT (firebase_uid)
      DO UPDATE SET
        name = EXCLUDED.name,
        email = EXCLUDED.email
      RETURNING *
      `,
      [firebase_uid, name || "", email]
    );

    return res.json({
      ok: true,
      user: result.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "USER SYNC ERROR:", err);
  }
});

app.get("/users/:firebase_uid", async (req, res) => {
  try {
    const { firebase_uid } = req.params;

    const result = await pool.query(
      "SELECT * FROM users WHERE firebase_uid = $1",
      [firebase_uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "User not found",
      });
    }

    return res.json({
      ok: true,
      user: result.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "GET USER ERROR:", err);
  }
});

app.post("/creator/activate-test", async (req, res) => {
  try {
    const { firebase_uid } = req.body;

    if (!firebase_uid) {
      return res.status(400).json({
        ok: false,
        error: "firebase_uid is required",
      });
    }

    const result = await pool.query(
      `
      UPDATE users
      SET is_creator = true
      WHERE firebase_uid = $1
      RETURNING *
      `,
      [firebase_uid]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "User not found",
      });
    }

    return res.json({
      ok: true,
      user: result.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "CREATOR ACTIVATE TEST ERROR:", err);
  }
});

app.post("/uploads/create-url", async (req, res) => {
  try {
    const {
      firebase_uid,
      video_file_name,
      thumbnail_file_name,
      video_content_type,
      thumbnail_content_type,
    } = req.body;

    if (!video_file_name || !thumbnail_file_name) {
      return res.status(400).json({
        ok: false,
        error: "video_file_name and thumbnail_file_name are required",
      });
    }

    await requireCreator(firebase_uid);

    const now = Date.now();

    const safeVideoName = cleanFileName(video_file_name);
    const safeThumbnailName = cleanFileName(thumbnail_file_name);

    const videoExt = getFileExtension(safeVideoName, ".mp4");
    const thumbnailExt = getFileExtension(safeThumbnailName, ".jpg");

    const videoKey = `videos/original/${firebase_uid}/${now}${videoExt}`;
    const thumbnailKey = `thumbnails/${firebase_uid}/${now}${thumbnailExt}`;

    const videoContentType = video_content_type || "video/mp4";
    const thumbnailContentType = thumbnail_content_type || "image/jpeg";

    const videoUploadUrl = await s3.getSignedUrlPromise("putObject", {
      Bucket: process.env.R2_BUCKET_NAME,
      Key: videoKey,
      ContentType: videoContentType,
      Expires: 60 * 60,
    });

    const thumbnailUploadUrl = await s3.getSignedUrlPromise("putObject", {
      Bucket: process.env.R2_BUCKET_NAME,
      Key: thumbnailKey,
      ContentType: thumbnailContentType,
      Expires: 60 * 60,
    });

    const videoUrl = `${process.env.R2_PUBLIC_URL}/${videoKey}`;
    const thumbnailUrl = `${process.env.R2_PUBLIC_URL}/${thumbnailKey}`;

    return res.json({
      ok: true,
      upload: {
        upload_type: "direct_put",
        video_upload_url: videoUploadUrl,
        thumbnail_upload_url: thumbnailUploadUrl,
        video_key: videoKey,
        thumbnail_key: thumbnailKey,
        video_url: videoUrl,
        thumbnail_url: thumbnailUrl,
        expires_in_seconds: 3600,
      },
    });
  } catch (err) {
    return handleRouteError(res, "CREATE UPLOAD URL ERROR:", err);
  }
});

app.post("/uploads/complete", async (req, res) => {
  try {
    const {
      firebase_uid,
      title,
      description,
      creator_name,
      video_key,
      video_url,
      thumbnail_key,
      thumbnail_url,
      video_size,
    } = req.body;

    if (!video_key || !video_url || !thumbnail_key || !thumbnail_url) {
      return res.status(400).json({
        ok: false,
        error: "video and thumbnail details are required",
      });
    }

    const user = await requireCreator(firebase_uid);

    const finalTitle =
      title && String(title).trim().length > 0
        ? String(title).trim()
        : "Untitled Video";

    const finalDescription = description || "";
    const finalCreatorName =
      creator_name && String(creator_name).trim().length > 0
        ? String(creator_name).trim()
        : user.name || user.email || "Unknown Creator";

    const result = await pool.query(
      `
      INSERT INTO videos (
        video_key,
        video_url,
        title,
        description,
        creator_uid,
        creator_name,
        thumbnail_key,
        thumbnail_url,
        views,
        processing_status,
        original_video_url,
        original_video_key,
        processed_video_url,
        processed_video_key,
        file_size_bytes
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'queued', $9, $10, $11, $12, $13)
      RETURNING *
      `,
      [
        video_key,
        video_url,
        finalTitle,
        finalDescription,
        firebase_uid,
        finalCreatorName,
        thumbnail_key,
        thumbnail_url,
        video_url,
        video_key,
        video_url,
        video_key,
        Number(video_size || 0),
      ]
    );

    return res.json({
      ok: true,
      message: "Upload completed and queued for HLS processing",
      video: result.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "UPLOAD COMPLETE ERROR:", err);
  }
});

app.post("/uploads/multipart/start", async (req, res) => {
  try {
    const {
      firebase_uid,
      video_file_name,
      thumbnail_file_name,
      video_content_type,
      thumbnail_content_type,
      video_size,
      thumbnail_size,
      title,
      description,
      creator_name,
    } = req.body;

    if (!video_file_name || !thumbnail_file_name) {
      return res.status(400).json({
        ok: false,
        error: "video_file_name and thumbnail_file_name are required",
      });
    }

    const user = await requireCreator(firebase_uid);

    const now = Date.now();

    const safeVideoName = cleanFileName(video_file_name);
    const safeThumbnailName = cleanFileName(thumbnail_file_name);

    const videoExt = getFileExtension(safeVideoName, ".mp4");
    const thumbnailExt = getFileExtension(safeThumbnailName, ".jpg");

    const videoContentType = video_content_type || "video/mp4";
    const thumbnailContentType = thumbnail_content_type || "image/jpeg";

    const videoKey = `videos/original/${firebase_uid}/${now}${videoExt}`;
    const thumbnailKey = `thumbnails/${firebase_uid}/${now}${thumbnailExt}`;

    const multipart = await s3
      .createMultipartUpload({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: videoKey,
        ContentType: videoContentType,
      })
      .promise();

    const uploadId = multipart.UploadId;

    const thumbnailUploadUrl = await s3.getSignedUrlPromise("putObject", {
      Bucket: process.env.R2_BUCKET_NAME,
      Key: thumbnailKey,
      ContentType: thumbnailContentType,
      Expires: 60 * 60,
    });

    const finalCreatorName =
      creator_name && String(creator_name).trim().length > 0
        ? String(creator_name).trim()
        : user.name || user.email || "Unknown Creator";

    await pool.query(
      `
      INSERT INTO upload_sessions (
        firebase_uid,
        upload_id,
        video_key,
        thumbnail_key,
        video_file_name,
        thumbnail_file_name,
        video_content_type,
        thumbnail_content_type,
        video_size,
        thumbnail_size,
        title,
        description,
        creator_name,
        status,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'started', CURRENT_TIMESTAMP)
      `,
      [
        firebase_uid,
        uploadId,
        videoKey,
        thumbnailKey,
        safeVideoName,
        safeThumbnailName,
        videoContentType,
        thumbnailContentType,
        Number(video_size || 0),
        Number(thumbnail_size || 0),
        title || "Untitled Video",
        description || "",
        finalCreatorName,
      ]
    );

    return res.json({
      ok: true,
      upload: {
        upload_type: "multipart",
        upload_id: uploadId,
        video_key: videoKey,
        thumbnail_key: thumbnailKey,
        video_url: `${process.env.R2_PUBLIC_URL}/${videoKey}`,
        thumbnail_url: `${process.env.R2_PUBLIC_URL}/${thumbnailKey}`,
        thumbnail_upload_url: thumbnailUploadUrl,
        part_url_expires_in_seconds: 3600,
      },
    });
  } catch (err) {
    return handleRouteError(res, "MULTIPART START ERROR:", err);
  }
});

app.post("/uploads/multipart/sign-part", async (req, res) => {
  try {
    const { firebase_uid, upload_id, video_key, part_number } = req.body;

    if (!upload_id || !video_key || !part_number) {
      return res.status(400).json({
        ok: false,
        error: "upload_id, video_key, and part_number are required",
      });
    }

    await requireCreator(firebase_uid);

    const session = await pool.query(
      `
      SELECT * FROM upload_sessions
      WHERE firebase_uid = $1 AND upload_id = $2 AND video_key = $3
      LIMIT 1
      `,
      [firebase_uid, upload_id, video_key]
    );

    if (session.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "Upload session not found",
      });
    }

    const partNumberInt = Number(part_number);

    if (!Number.isInteger(partNumberInt) || partNumberInt < 1) {
      return res.status(400).json({
        ok: false,
        error: "Invalid part_number",
      });
    }

    const partUploadUrl = await s3.getSignedUrlPromise("uploadPart", {
      Bucket: process.env.R2_BUCKET_NAME,
      Key: video_key,
      UploadId: upload_id,
      PartNumber: partNumberInt,
      Expires: 60 * 60,
    });

    await pool.query(
      `
      UPDATE upload_sessions
      SET status = 'uploading', updated_at = CURRENT_TIMESTAMP
      WHERE firebase_uid = $1 AND upload_id = $2 AND video_key = $3
      `,
      [firebase_uid, upload_id, video_key]
    );

    return res.json({
      ok: true,
      part: {
        part_number: partNumberInt,
        upload_url: partUploadUrl,
        expires_in_seconds: 3600,
      },
    });
  } catch (err) {
    return handleRouteError(res, "MULTIPART SIGN PART ERROR:", err);
  }
});

app.post("/uploads/multipart/complete", async (req, res) => {
  try {
    const {
      firebase_uid,
      upload_id,
      video_key,
      thumbnail_key,
      thumbnail_url,
      parts,
    } = req.body;

    if (!upload_id || !video_key || !Array.isArray(parts) || parts.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "upload_id, video_key, and parts are required",
      });
    }

    const user = await requireCreator(firebase_uid);

    const sessionResult = await pool.query(
      `
      SELECT * FROM upload_sessions
      WHERE firebase_uid = $1 AND upload_id = $2 AND video_key = $3
      LIMIT 1
      `,
      [firebase_uid, upload_id, video_key]
    );

    if (sessionResult.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "Upload session not found",
      });
    }

    const session = sessionResult.rows[0];

    const sortedParts = parts
      .map((part) => ({
        PartNumber: Number(part.part_number),
        ETag: String(part.etag || "").replace(/^"|"$/g, ""),
      }))
      .filter((part) => part.PartNumber > 0 && part.ETag.length > 0)
      .sort((a, b) => a.PartNumber - b.PartNumber);

    if (sortedParts.length === 0) {
      return res.status(400).json({
        ok: false,
        error: "Valid uploaded parts are required",
      });
    }

    await s3
      .completeMultipartUpload({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: video_key,
        UploadId: upload_id,
        MultipartUpload: {
          Parts: sortedParts,
        },
      })
      .promise();

    const finalVideoUrl = `${process.env.R2_PUBLIC_URL}/${video_key}`;
    const finalThumbnailKey = thumbnail_key || session.thumbnail_key;
    const finalThumbnailUrl =
      thumbnail_url || `${process.env.R2_PUBLIC_URL}/${finalThumbnailKey}`;

    const finalTitle =
      session.title && String(session.title).trim().length > 0
        ? String(session.title).trim()
        : "Untitled Video";

    const finalDescription = session.description || "";
    const finalCreatorName =
      session.creator_name && String(session.creator_name).trim().length > 0
        ? String(session.creator_name).trim()
        : user.name || user.email || "Unknown Creator";

    const videoResult = await pool.query(
      `
      INSERT INTO videos (
        video_key,
        video_url,
        title,
        description,
        creator_uid,
        creator_name,
        thumbnail_key,
        thumbnail_url,
        views,
        processing_status,
        original_video_url,
        original_video_key,
        processed_video_url,
        processed_video_key,
        file_size_bytes
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'queued', $9, $10, $11, $12, $13)
      RETURNING *
      `,
      [
        video_key,
        finalVideoUrl,
        finalTitle,
        finalDescription,
        firebase_uid,
        finalCreatorName,
        finalThumbnailKey,
        finalThumbnailUrl,
        finalVideoUrl,
        video_key,
        finalVideoUrl,
        video_key,
        Number(session.video_size || 0),
      ]
    );

    await pool.query(
      `
      UPDATE upload_sessions
      SET status = 'completed', updated_at = CURRENT_TIMESTAMP
      WHERE firebase_uid = $1 AND upload_id = $2 AND video_key = $3
      `,
      [firebase_uid, upload_id, video_key]
    );

    return res.json({
      ok: true,
      message: "Multipart upload completed and queued for HLS processing",
      video: videoResult.rows[0],
    });
  } catch (err) {
    return handleRouteError(res, "MULTIPART COMPLETE ERROR:", err);
  }
});

app.post("/uploads/multipart/abort", async (req, res) => {
  try {
    const { firebase_uid, upload_id, video_key } = req.body;

    if (!upload_id || !video_key) {
      return res.status(400).json({
        ok: false,
        error: "upload_id and video_key are required",
      });
    }

    await requireCreator(firebase_uid);

    await s3
      .abortMultipartUpload({
        Bucket: process.env.R2_BUCKET_NAME,
        Key: video_key,
        UploadId: upload_id,
      })
      .promise();

    await pool.query(
      `
      UPDATE upload_sessions
      SET status = 'aborted', updated_at = CURRENT_TIMESTAMP
      WHERE firebase_uid = $1 AND upload_id = $2 AND video_key = $3
      `,
      [firebase_uid, upload_id, video_key]
    );

    return res.json({
      ok: true,
      message: "Multipart upload aborted",
    });
  } catch (err) {
    return handleRouteError(res, "MULTIPART ABORT ERROR:", err);
  }
});

app.post(
  "/upload",
  upload.fields([
    { name: "video", maxCount: 1 },
    { name: "thumbnail", maxCount: 1 },
  ]),
  async (req, res) => {
    let inputPath;
    let outputPath;
    let thumbnailPath;

    try {
      const videoFile = req.files?.video?.[0];
      const thumbnailFile = req.files?.thumbnail?.[0];

      if (!videoFile) {
        return res.status(400).json({ error: "No video uploaded" });
      }

      if (!thumbnailFile) {
        return res.status(400).json({ error: "No thumbnail uploaded" });
      }

      const title = req.body.title || "Untitled Video";
      const description = req.body.description || "";
      const creatorUid = req.body.creator_uid || "";
      const creatorName = req.body.creator_name || "Unknown Creator";

      inputPath = videoFile.path;
      thumbnailPath = thumbnailFile.path;
      outputPath = path.join(outputDir, `${Date.now()}.mp4`);

      const ffmpegCommand = `ffmpeg -y -i "${inputPath}" -vf "scale=1280:-2" -preset fast -movflags +faststart "${outputPath}"`;

      exec(ffmpegCommand, async (ffmpegError, stdout, stderr) => {
        if (ffmpegError) {
          console.error("FFMPEG ERROR:", ffmpegError);
          console.error("FFMPEG STDERR:", stderr);

          if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
          if (thumbnailPath && fs.existsSync(thumbnailPath)) fs.unlinkSync(thumbnailPath);
          if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

          return res.status(500).json({
            error: "FFmpeg failed",
            details: stderr,
          });
        }

        try {
          const now = Date.now();

          const videoKey = `videos/processed/${creatorUid || "unknown"}/${now}.mp4`;
          const videoUrl = `${process.env.R2_PUBLIC_URL}/${videoKey}`;

          const thumbnailExt = getFileExtension(thumbnailFile.originalname, ".jpg");
          const thumbnailKey = `thumbnails/${creatorUid || "unknown"}/${now}${thumbnailExt}`;
          const thumbnailUrl = `${process.env.R2_PUBLIC_URL}/${thumbnailKey}`;

          const outputSize = fs.existsSync(outputPath)
            ? fs.statSync(outputPath).size
            : 0;

          await s3
            .upload({
              Bucket: process.env.R2_BUCKET_NAME,
              Key: videoKey,
              Body: fs.createReadStream(outputPath),
              ContentType: "video/mp4",
            })
            .promise();

          await s3
            .upload({
              Bucket: process.env.R2_BUCKET_NAME,
              Key: thumbnailKey,
              Body: fs.createReadStream(thumbnailPath),
              ContentType: thumbnailFile.mimetype || "image/jpeg",
            })
            .promise();

          await pool.query(
            `
            INSERT INTO videos (
              video_key,
              video_url,
              title,
              description,
              creator_uid,
              creator_name,
              thumbnail_key,
              thumbnail_url,
              views,
              processing_status,
              original_video_url,
              original_video_key,
              processed_video_url,
              processed_video_key,
              file_size_bytes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 0, 'ready', $9, $10, $11, $12, $13)
            `,
            [
              videoKey,
              videoUrl,
              title,
              description,
              creatorUid,
              creatorName,
              thumbnailKey,
              thumbnailUrl,
              videoUrl,
              videoKey,
              videoUrl,
              videoKey,
              outputSize,
            ]
          );

          if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
          if (fs.existsSync(thumbnailPath)) fs.unlinkSync(thumbnailPath);
          if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

          return res.json({
            message: "Upload success",
            key: videoKey,
            url: videoUrl,
            thumbnail_key: thumbnailKey,
            thumbnail_url: thumbnailUrl,
            title,
            description,
            creator_uid: creatorUid,
            creator_name: creatorName,
          });
        } catch (error) {
          console.error("UPLOAD SAVE ERROR:", error);

          if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
          if (thumbnailPath && fs.existsSync(thumbnailPath)) fs.unlinkSync(thumbnailPath);
          if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

          return res.status(500).json({
            error: "Upload or DB save failed",
            details: error.message,
          });
        }
      });
    } catch (error) {
      console.error("UPLOAD ROUTE ERROR:", error);

      if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
      if (thumbnailPath && fs.existsSync(thumbnailPath)) fs.unlinkSync(thumbnailPath);
      if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

      return res.status(500).json({
        error: "Upload failed",
        details: error.message,
      });
    }
  }
);

let isAutoWorkerRunning = false;

async function processNextQueuedVideo() {
  if (isAutoWorkerRunning) {
    return;
  }

  isAutoWorkerRunning = true;

  try {
    const video = await claimNextQueuedVideo();

    if (!video) {
      return;
    }

    if (!video.original_video_url) {
      await pool.query(
        `
        UPDATE videos
        SET processing_status = 'failed',
            processing_error = 'original_video_url missing',
            processing_started_at = NULL,
            processing_worker_id = NULL
        WHERE id = $1
        `,
        [video.id]
      );
      return;
    }

    console.log(`SAFE AUTO WORKER ${WORKER_ID} processing video ${video.id}`);

    await processVideo(video);

    console.log(`SAFE AUTO WORKER ${WORKER_ID} finished video ${video.id}`);
  } catch (error) {
    console.error("SAFE AUTO WORKER ERROR:", error);
  } finally {
    isAutoWorkerRunning = false;
  }
}

if (AUTO_WORKER_ENABLED) {
  console.log(
    `Safe auto worker enabled. worker_id=${WORKER_ID}, interval_ms=${AUTO_WORKER_INTERVAL_MS}`
  );

  setInterval(() => {
    processNextQueuedVideo();
  }, AUTO_WORKER_INTERVAL_MS);
} else {
  console.log("Safe auto worker disabled by AUTO_WORKER_ENABLED=false");
}

const PORT = process.env.PORT || 8080;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});