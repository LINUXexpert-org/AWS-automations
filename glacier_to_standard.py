import boto3
import time
import math
import logging

# === Configuration ===
BUCKET_NAME = "your-bucket-name"
S3_ENDPOINT_URL = "https://s3.your-region.amazonaws.com"  # Update as needed
PART_SIZE = 500 * 1024 * 1024  # 500MB
LOG_FILE = "glacier_to_standard.log"

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


def get_glacier_objects():
    glacier_objects = []
    paginator = s3.get_paginator("list_objects_v2")
    logging.info("Scanning for GLACIER objects...")

    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            if obj.get("StorageClass") == "GLACIER":
                glacier_objects.append(obj["Key"])

    logging.info(f"Found {len(glacier_objects)} GLACIER objects.")
    return glacier_objects


def initiate_restore(keys):
    for key in keys:
        try:
            head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            if "Restore" in head and 'ongoing-request="true"' in head["Restore"]:
                logging.info(f"Restore already in progress: {key}")
            elif "Restore" in head and 'ongoing-request="false"' in head["Restore"]:
                logging.info(f"Already restored: {key}")
            else:
                s3.restore_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    RestoreRequest={
                        "Days": 1,
                        "GlacierJobParameters": {"Tier": "Standard"}
                    }
                )
                logging.info(f"Restore initiated: {key}")
        except Exception as e:
            logging.error(f"Error restoring {key}: {e}")


def wait_for_restore(keys):
    logging.info("Waiting for restoration to complete (checking every 60s)...")
    restored = {}

    while len(restored) < len(keys):
        for key in keys:
            if key in restored:
                continue
            try:
                head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
                if "Restore" in head and 'ongoing-request="false"' in head["Restore"]:
                    size = head["ContentLength"]
                    restored[key] = size
                    logging.info(f"Restored: {key} ({size} bytes)")
            except Exception as e:
                logging.error(f"Error checking {key}: {e}")

        logging.info(f"{len(restored)} of {len(keys)} objects restored.")
        if len(restored) < len(keys):
            time.sleep(60)

    return restored


def multipart_copy(key, size):
    logging.info(f"Multipart copying {key} ({size} bytes)...")
    try:
        upload = s3.create_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=key,
            StorageClass="STANDARD",
            MetadataDirective="COPY"
        )
        part_count = math.ceil(size / PART_SIZE)
        parts = []

        for i in range(part_count):
            start = i * PART_SIZE
            end = min(start + PART_SIZE - 1, size - 1)

            part = s3.upload_part_copy(
                Bucket=BUCKET_NAME,
                Key=key,
                PartNumber=i + 1,
                UploadId=upload["UploadId"],
                CopySource={"Bucket": BUCKET_NAME, "Key": key},
                CopySourceRange=f"bytes={start}-{end}"
            )
            parts.append({"ETag": part["CopyPartResult"]["ETag"], "PartNumber": i + 1})
            logging.info(f"Copied part {i+1}/{part_count} for {key}")

        s3.complete_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=key,
            UploadId=upload["UploadId"],
            MultipartUpload={"Parts": parts}
        )
        logging.info(f"Completed multipart copy for {key}")
    except Exception as e:
        logging.error(f"Multipart copy failed for {key}: {e}")
        try:
            s3.abort_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=upload["UploadId"])
        except:
            logging.warning(f"Could not abort upload for {key}")


def cleanup_glacier_versions(key):
    """Delete older versions in Glacier/Deep Archive (if versioning is enabled)."""
    try:
        versions = s3.list_object_versions(Bucket=BUCKET_NAME, Prefix=key)
        for v in versions.get("Versions", []):
            if v.get("IsLatest"):
                continue
            if v.get("StorageClass") in ["GLACIER", "DEEP_ARCHIVE"]:
                version_id = v["VersionId"]
                s3.delete_object(Bucket=BUCKET_NAME, Key=key, VersionId=version_id)
                logging.info(f"Deleted Glacier version: {key} (version {version_id})")
    except Exception as e:
        logging.error(f"Error cleaning up versions for {key}: {e}")


def transition_to_standard(restored_objects):
    logging.info("Transitioning restored objects to STANDARD storage...")
    for key, size in restored_objects.items():
        try:
            if size > 5 * 1024 * 1024 * 1024:
                multipart_copy(key, size)
            else:
                s3.copy_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    CopySource={"Bucket": BUCKET_NAME, "Key": key},
                    StorageClass="STANDARD",
                    MetadataDirective="COPY"
                )
                logging.info(f"Standard copy complete for: {key}")

            cleanup_glacier_versions(key)

        except Exception as e:
            logging.error(f"Failed to transition {key} to STANDARD: {e}")


def main():
    logging.info(f"Starting Glacier → STANDARD transition for bucket: {BUCKET_NAME}")
    glacier_keys = get_glacier_objects()

    if not glacier_keys:
        logging.info("No GLACIER objects found.")
        return

    initiate_restore(glacier_keys)
    restored_objects = wait_for_restore(glacier_keys)
    transition_to_standard(restored_objects)
    logging.info("All eligible objects transitioned and cleaned up.")


if __name__ == "__main__":
    main()
