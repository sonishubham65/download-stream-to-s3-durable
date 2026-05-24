import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { withDurableExecution } from "@aws/durable-execution-sdk-js";

const s3 = new S3Client({});

// 20 MB parts
const PART_SIZE = 20 * 1024 * 1024;

export const handler = withDurableExecution(async (event, context) => {
  const bucket = "ng-download-stream-to-s3";
  const key = "test/1Gb.dat";

  const url = "https://proof.ovh.net/files/1Gb.dat";

  context.logger.info("Starting multipart durable download");

  let uploadId;

  try {
    /**
     * STEP 1: Create multipart upload
     */
    uploadId = await context.step("Create Multipart Upload", async () => {
      const response = await s3.send(
        new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
        }),
      );

      if (!response.UploadId) {
        throw new Error("UploadId missing from S3");
      }

      return response.UploadId;
    });

    context.logger.info(`UploadId: ${uploadId}`);

    /**
     * STEP 2: Read total file size
     */
    const totalSize = await context.step("Read file total size", async () => {
      const response = await fetch(url, {
        headers: {
          Range: "bytes=0-0",
        },
      });

      if (response.status !== 206) {
        throw new Error(
          `Failed to determine file size. Status: ${response.status}`,
        );
      }

      const contentRange = response.headers.get("content-range");

      if (!contentRange) {
        throw new Error("Missing content-range header");
      }

      /**
       * Example:
       * bytes 0-0/1073741824
       */
      const totalSize = Number(contentRange.split("/")[1]);

      if (!Number.isFinite(totalSize)) {
        throw new Error(
          `Invalid total size from content-range: ${contentRange}`,
        );
      }

      return totalSize;
    });

    const totalParts = Math.ceil(totalSize / PART_SIZE);

    context.logger.info(`Total Size: ${totalSize}, Total Parts: ${totalParts}`);

    const completedParts = [];

    /**
     * STEP 3: Upload parts
     */
    for (let partNumber = 1; partNumber <= totalParts; partNumber++) {
      const start = (partNumber - 1) * PART_SIZE;

      const end = Math.min(start + PART_SIZE - 1, totalSize - 1);

      const contentLength = end - start + 1;

      const uploadedPart = await context.step(
        `Upload Part ${partNumber}`,
        async (stepCtx) => {
          stepCtx.logger.info(
            JSON.stringify({
              partNumber,
              start,
              end,
              contentLength,
            }),
          );

          const response = await fetch(url, {
            headers: {
              Range: `bytes=${start}-${end}`,
            },
          });

          /**
           * Range requests should return 206
           */
          if (response.status !== 206) {
            throw new Error(
              `Invalid response status ${response.status} for part ${partNumber}`,
            );
          }

          if (!response.body) {
            throw new Error(`Missing response body for part ${partNumber}`);
          }

          const body = Readable.fromWeb(response.body);

          stepCtx.logger.info(`Uploading part ${partNumber}`);

          const uploadPartResponse = await s3.send(
            new UploadPartCommand({
              Bucket: bucket,
              Key: key,
              UploadId: uploadId,
              PartNumber: partNumber,
              Body: body,
              ContentLength: contentLength,
            }),
          );

          if (!uploadPartResponse.ETag) {
            throw new Error(`Missing ETag for part ${partNumber}`);
          }

          stepCtx.logger.info(`Completed part ${partNumber}`);

          return {
            ETag: uploadPartResponse.ETag,
            PartNumber: partNumber,
          };
        },
      );

      completedParts.push(uploadedPart);
    }

    /**
     * STEP 4: Complete upload
     */
    await context.step("Complete Multipart Upload", async () => {
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
    });

    context.logger.info("Multipart upload completed");

    return {
      statusCode: 200,
      body: JSON.stringify({
        success: true,
        totalSize,
        totalParts,
        bucket,
        key,
      }),
    };
  } catch (error) {
    context.logger.error("Transfer failed", error);

    /**
     * Cleanup upload safely
     */
    if (uploadId) {
      try {
        await context.step("Abort Multipart Upload", async () => {
          await s3.send(
            new AbortMultipartUploadCommand({
              Bucket: bucket,
              Key: key,
              UploadId: uploadId,
            }),
          );
        });
      } catch (abortError) {
        context.logger.error("Failed aborting multipart upload", abortError);
      }
    }

    throw error;
  }
});
