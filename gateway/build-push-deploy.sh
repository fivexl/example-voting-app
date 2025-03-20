#!/bin/bash

# Get AWS account ID and current region
ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text)
REGION=us-east-1  # Hardcoded to us-east-1 (change if needed)

# Get input parameters
REPO_NAME=gateway
CLUSTER_NAME=services
SERVICE_NAME=gateway
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

# Force ECS service to deploy a new task
echo "Forcing new deployment..."
aws ecs update-service --cluster "$CLUSTER_NAME" --service "$SERVICE_NAME" --force-new-deployment

# Get the current running tasks
TASK_ARNS=$(aws ecs list-tasks --cluster "$CLUSTER_NAME" --service-name "$SERVICE_NAME" --query "taskArns" --output text)

# Stop all existing tasks
if [[ -n "$TASK_ARNS" ]]; then
  echo "Stopping existing tasks..."
  for TASK_ARN in $TASK_ARNS; do
    aws ecs stop-task --cluster "$CLUSTER_NAME" --task "$TASK_ARN" --reason "Deploying new version"
  done
else
  echo "No existing tasks found."
fi

# Wait until a new task is running
echo "Waiting for new task to start..."
while true; do
  NEW_TASK_ARN=$(aws ecs list-tasks --cluster "$CLUSTER_NAME" --service-name "$SERVICE_NAME" --query "taskArns[0]" --output text)
  if [[ -n "$NEW_TASK_ARN" ]]; then
    STATUS=$(aws ecs describe-tasks --cluster "$CLUSTER_NAME" --tasks "$NEW_TASK_ARN" --query "tasks[0].lastStatus" --output text)
    echo "Current task status: $STATUS"
    if [[ "$STATUS" == "RUNNING" ]]; then
      echo "New task is up and running: $NEW_TASK_ARN"
      break
    fi
  fi
  sleep 5
done

echo "Deployment complete."
