#!/bin/bash

# Get AWS account ID and current region
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
REGION=us-east-1  # Hardcoded to us-east-1 (change if needed)

# Get input parameters
REPO_NAME=gateway
TAG=${1:-latest}  # Default to 'latest' if no tag is provided

if [[ -z "$REPO_NAME" ]]; then
  echo "Usage: $0 <tag>"
  exit 1
fi

# Authenticate Docker with ECR
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"

# Build image explicitly for linux/amd64
docker build --platform linux/amd64 -t "$REPO_NAME" .

# Tag and push the image
docker tag "$REPO_NAME:latest" "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$TAG"
docker push "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$TAG"

echo "Image pushed to: $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:$TAG"
