"""
Quick smoke test for MinIO connection.
Usage: py test_minio.py
"""
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import os

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    config=Config(signature_version="s3v4"),
)

bucket = os.environ["MINIO_BUCKET"]

# Create bucket if it doesn't exist
existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if bucket not in existing:
    s3.create_bucket(Bucket=bucket)
    print(f"✅ Created bucket: {bucket}")
else:
    print(f"✅ Bucket already exists: {bucket}")

# Upload a test file
s3.put_object(
    Bucket=bucket,
    Key="test/hello.json",
    Body=b'{"message": "MinIO is working!"}',
    ContentType="application/json",
)
print("✅ Test file uploaded")

# Read it back
obj = s3.get_object(Bucket=bucket, Key="test/hello.json")
content = obj["Body"].read().decode()
print(f"✅ Read back: {content}")

# Cleanup
s3.delete_object(Bucket=bucket, Key="test/hello.json")
print("✅ Cleaned up")

print("\n🎉 MinIO is fully working!")