const express = require("express");
const multer = require("multer");
const cors = require("cors");
const { exec } = require("child_process");
const fs = require("fs");
const path = require("path");
const AWS = require("aws-sdk");

const app = express();
app.use(cors());
app.use(express.json());

const uploadDir = "/tmp/uploads";
const outputDir = "/tmp/output";

fs.mkdirSync(uploadDir, { recursive: true });
fs.mkdirSync(outputDir, { recursive: true });

const upload = multer({ dest: uploadDir });

const requiredEnv = [
  "R2_ACCOUNT_ID",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
  "R2_BUCKET_NAME",
  "R2_PUBLIC_URL",
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
  });
});

app.post("/upload", upload.single("video"), async (req, res) => {
  let inputPath;
  let outputPath;

  try {
    if (!req.file) {
      return res.status(400).json({ error: "No video uploaded" });
    }

    inputPath = req.file.path;
    outputPath = path.join(outputDir, `${Date.now()}.mp4`);

    const ffmpegCommand = `ffmpeg -y -i "${inputPath}" -vf "scale=1280:-2" -preset fast -movflags +faststart "${outputPath}"`;

    exec(ffmpegCommand, async (ffmpegError, stdout, stderr) => {
      if (ffmpegError) {
        console.error("FFMPEG ERROR:", ffmpegError);
        console.error("FFMPEG STDERR:", stderr);
        return res.status(500).json({
          error: "FFmpeg failed",
          details: stderr,
        });
      }

      try {
        const key = `videos/${Date.now()}.mp4`;

        await s3
          .upload({
            Bucket: process.env.R2_BUCKET_NAME,
            Key: key,
            Body: fs.createReadStream(outputPath),
            ContentType: "video/mp4",
          })
          .promise();

        if (fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
        if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

        return res.json({
          message: "Upload success",
          key,
          url: `${process.env.R2_PUBLIC_URL}/${key}`,
        });
      } catch (r2Error) {
        console.error("R2 UPLOAD ERROR:", r2Error);

        return res.status(500).json({
          error: "R2 upload failed",
          details: r2Error.message,
          code: r2Error.code,
        });
      }
    });
  } catch (error) {
    console.error("UPLOAD ROUTE ERROR:", error);

    if (inputPath && fs.existsSync(inputPath)) fs.unlinkSync(inputPath);
    if (outputPath && fs.existsSync(outputPath)) fs.unlinkSync(outputPath);

    return res.status(500).json({
      error: "Upload failed",
      details: error.message,
    });
  }
});

const PORT = process.env.PORT || 8080;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});