# Stream Large Files to Amazon S3 Using Multipart Uploads and Durable Execution

Efficiently download multi-GB files from external sources and upload them directly to Amazon S3 in chunks using streaming, multipart uploads, and durable execution.

---

## 🚀 Overview

This project demonstrates how to:

- Download large files from external URLs
- Stream data directly to Amazon S3
- Use S3 Multipart Upload for scalability
- Avoid loading entire files into memory
- Support resumable and durable execution
- Handle failures safely with automatic cleanup

Perfect for:

- Media ingestion pipelines
- Large backup transfers
- Cross-cloud migrations
- ETL pipelines
- Data lake ingestion
- B2B catalog imports
- Serverless file processing workflows

---

## 🏗 Architecture

```text
External File Source
        │
        ▼
HTTP Range Request
        │
        ▼
Lambda Stream Pipeline
        │
        ▼
S3 Multipart Upload
        │
        ▼
Final S3 Object
```

---

## ✨ Features

- ✅ Multipart upload support
- ✅ Range-based chunk downloading
- ✅ Streaming upload to S3
- ✅ Constant memory usage
- ✅ Durable execution workflow
- ✅ Fault-tolerant retries
- ✅ Automatic multipart cleanup
- ✅ No temporary file storage required

---

## 📦 Tech Stack

- Node.js
- AWS Lambda
- Amazon S3
- AWS SDK v3
- Durable Execution SDK
- Streams API

---

# 🧠 How It Works

## 1. Create Multipart Upload

```js
const response = await s3.send(
  new CreateMultipartUploadCommand({
    Bucket: bucket,
    Key: key,
  }),
);
```

---

## 2. Determine File Size

```js
const response = await fetch(url, {
  headers: {
    Range: "bytes=0-0",
  },
});
```

---

## 3. Split Into Chunks

```js
const PART_SIZE = 20 * 1024 * 1024;
```

---

## 4. Stream Chunks Directly to S3

```js
await s3.send(
  new UploadPartCommand({
    Bucket: bucket,
    Key: key,
    UploadId: uploadId,
    PartNumber: partNumber,
    Body: body,
    ContentLength: contentLength,
  }),
);
```

---

## 5. Complete Multipart Upload

```js
await s3.send(
  new CompleteMultipartUploadCommand({
    Bucket: bucket,
    Key: key,
    UploadId: uploadId,
    MultipartUpload: {
      Parts: completedParts,
    },
  }),
);
```

---

# 🔥 Durable Execution

This project uses durable execution to safely resume uploads from failed steps.

Benefits:

- Retry-safe workflows
- Resume from failed uploads
- Fault-tolerant processing
- Reliable large file transfer

---

# ⚙️ Installation

## Clone Repository

```bash
git clone https://github.com/your-username/stream-to-s3-multipart.git

cd stream-to-s3-multipart
```

## Install Dependencies

```bash
npm install
```

---

# 🔑 AWS Permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:CreateMultipartUpload",
    "s3:UploadPart",
    "s3:CompleteMultipartUpload",
    "s3:AbortMultipartUpload"
  ],
  "Resource": "*"
}
```

---

# ▶️ Usage

Update:

```js
const bucket = "your-bucket-name";
const key = "path/file.dat";

const url = "https://example.com/file.dat";
```

Then deploy and invoke the Lambda.

---

# 🛡 Production Recommendations

- Add retry backoff
- Add observability
- Add concurrency controls
- Add checksum validation
- Add metrics and monitoring

---

# 📄 License

MIT License

---

# ⭐ Support

If this helped you, consider starring the repository 🚀
