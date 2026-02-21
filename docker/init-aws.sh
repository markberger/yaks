#!/bin/sh
set -e # Exit immediately if a command exits with a non-zero status.

BUCKET_NAME="test-bucket" # Define your bucket name here

echo "Executing LocalStack init script: Creating S3 bucket '$BUCKET_NAME'..."
awslocal s3 mb "s3://${BUCKET_NAME}"
echo "S3 bucket '$BUCKET_NAME' creation command executed."
