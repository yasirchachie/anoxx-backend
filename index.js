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

fs.mkdirSync("uploads", { recursive: true });
fs.mkdirSync("output", { recursive: true });

const upload = multer({ dest: "uploads/" });

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

app.post("/upload", upload.single("video"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No video uploaded" });
  }

  const inputPath = req.file.path;
  const outputPath = path.join("output", `${Date.now()}.mp4`);

  const ffmpegCommand = `ffmpeg -y -i "${inputPath}" -vf scale=1280:720 -preset fast "${outputPath}"`;

  exec(ffmpegCommand, async (error) => {
    if (error) {
      console.error("FFmpeg error:", error);
      return res.status(500).json({ error: "FFmpeg error" });
    }

    try {
      const fileContent = fs.readFileSync(outputPath);
      const key = `videos/${Date.now()}.mp4`;

      await s3
        .upload({
          Bucket: process.env.R2_BUCKET_NAME,
          Key: key,
          Body: fileContent,
          ContentType: "video/mp4",
        })
        .promise();

      fs.unlinkSync(inputPath);
      fs.unlinkSync(outputPath);

      res.json({
        message: "Upload success",
        url: `${process.env.R2_PUBLIC_URL}/${key}`,
      });
    } catch (err) {
      console.error("R2 upload error:", err);
      res.status(500).json({ error: "Upload failed" });
    }
  });
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});