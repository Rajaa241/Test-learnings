#!/bin/bash

# Step 1: Delete the configuration in flink-job-deployment.yaml
echo "Deleting existing Flink job deployment..."
kubectl delete -f kubernetes/resources/flink-job-deployment.yaml

# Step 2: Apply the configuration in flink-job-deployment.yaml
echo "Applying new Flink job deployment..."
kubectl apply -f kubernetes/resources/flink-job-deployment.yaml

# Step 3: Wait for the pod to be created and retrieve its name
echo "Waiting for the pod matching sample-flink-job-taskmanager-* to be created..."
RETRY_COUNT=0
MAX_RETRIES=30
POD_NAME=""

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  POD_NAME=$(kubectl get pods --no-headers -o custom-columns=":metadata.name" | grep '^sample-flink-job-taskmanager-')
  if [ -n "$POD_NAME" ]; then
    break
  fi
  echo "Pod not found yet. Retrying in 2 seconds..."
  sleep 2
  RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ -z "$POD_NAME" ]; then
  echo "Failed to find a pod matching sample-flink-job-taskmanager-* after $((MAX_RETRIES * 2)) seconds."
  exit 1
fi

# Step 4: Wait for the container to start if it's in the "ContainerCreating" state
echo "Checking if the container is ready..."
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  CONTAINER_STATUS=$(kubectl get pod "$POD_NAME" -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null)
  if [ "$CONTAINER_STATUS" != "ContainerCreating" ]; then
    break
  fi
  echo "Container is still creating. Retrying in 2 seconds..."
  sleep 2
  RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ "$CONTAINER_STATUS" == "ContainerCreating" ]; then
  echo "Container is still in 'ContainerCreating' state after $((MAX_RETRIES * 2)) seconds."
  exit 1
fi

# Step 5: Port forward the pod to local port 5006
echo "Port forwarding pod $POD_NAME to local port 5006..."
kubectl port-forward "pod/$POD_NAME" 5006:5005 &

PORT_FORWARD_PID=$!

sleep 2

# Step 6: Fetch logs for the retrieved pod
echo "Fetching logs for pod: $POD_NAME"
kubectl logs "$POD_NAME" -f

# Step 7: Clean up port forwarding when logs are terminated
echo "Terminating port forwarding..."
kill $PORT_FORWARD_PID