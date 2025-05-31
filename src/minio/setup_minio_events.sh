#!/bin/sh

set -ex

# Wait until MinIO is ready
echo "⏳ Waiting for MinIO to be available..."
until curl -s http://minio:9000/minio/health/ready; do
    sleep 3
done
echo "✅ MinIO is ready."

# Wait until Kafka is ready
echo "⏳ Waiting for Kafka to be available on kafka:9092..."
until nc -z kafka 9092; do
    echo "Still waiting for Kafka broker on kafka:9092..."
    sleep 5
done
echo "✅ Kafka is ready."

echo "Configuring mc and event notification..."

# Set alias
mc alias set localminio http://minio:9000 "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

# --- Configure Kafka Target on MinIO Server - Set only essential and working properties ---
echo "Setting MinIO Kafka notifier configuration..."

# Set core properties first (without enable)
mc admin config set localminio notify_kafka:kafka brokers="kafka:9092" topic="retail-events"

# Explicitly enable the notifier in a *separate* command, trying 'on'
echo "Explicitly setting enable=on for Kafka notifier..."
mc admin config set localminio notify_kafka:kafka enable="on" # <--- CHANGED FROM "true" TO "on"

# Keep other specific settings if needed, but ensure 'enable' is set separately
mc admin config set localminio notify_kafka:kafka sasl_enable="false"
mc admin config set localminio notify_kafka:kafka tls_enable="false"


# --- CRITICAL: Apply configuration changes and restart MinIO service ---
echo "🔄 Restarting MinIO service to apply new configuration..."
mc admin service restart localminio 2>&1 | grep -v "could not open a new TTY" || echo "MinIO service restart command completed (may have other errors)."

# --- Wait until MinIO has loaded the Kafka notifier configuration and be enabled ---
echo "⏳ Waiting for MinIO to load Kafka notifier configuration and be enabled..."
MAX_RETRIES=10
RETRY_COUNT=0
until [ $RETRY_COUNT -ge $MAX_RETRIES ]; do
    echo "--- Debugging: Output of mc admin config get localminio notify_kafka:kafka ---"
    CONFIG_OUTPUT=$(mc admin config get localminio notify_kafka:kafka 2>/dev/null)
    echo "$CONFIG_OUTPUT"
    echo "----------------------------------------------------------------------"

    # Check if 'enable=true' OR 'enable=on' AND 'brokers' are present
    if echo "$CONFIG_OUTPUT" | grep -q "brokers" && (echo "$CONFIG_OUTPUT" | grep -q "enable=true" || echo "$CONFIG_OUTPUT" | grep -q "enable=on"); then
        echo "✅ MinIO Kafka notifier config is active and enabled."
        break # Exit the loop if both conditions are met
    fi

    echo "Still waiting for Kafka notifier config to be active and enabled..."
    sleep 5
    RETRY_COUNT=$((RETRY_COUNT+1))
done

# If the loop exited due to timeout, report an error
if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "❌ Timeout waiting for MinIO to load Kafka notifier config and enable it. The Kafka event target might not be active or enabled."
    exit 1 # Exit with an error if the config never becomes active
fi

# --- DIAGNOSTIC STEP: Check the specific notify_kafka configuration ---
echo "ℹ️ Checking specific notify_kafka configuration after enable attempt..."
mc admin config get localminio notify_kafka # This should now show enable=true/on
echo "--- End of notify_kafka configuration check ---"


# Add event notification for bucket
echo "Attempting to add bucket event notification for Kafka..."
mc event add localminio/${MINIO_BUCKET} arn:minio:kafka:::kafka
echo "Finished attempting to add bucket event notification."

echo "✅ MinIO event setup complete."