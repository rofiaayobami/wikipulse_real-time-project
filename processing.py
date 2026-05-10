
import json
import logging
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

from google.cloud import pubsub_v1
from pymongo import MongoClient

load_dotenv()

key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if key_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path


# Configuration
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
SUBSCRIPTION_ID = os.environ.get("SUBSCRIPTION_ID")
MONGO_URI = os.environ.get("MONGO_URI")

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, SUBSCRIPTION_ID)

if not all([GCP_PROJECT_ID, SUBSCRIPTION_ID, MONGO_URI]):
    raise ValueError("Missing required environment variables.")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("processor")

# MongoDB Setup
mongo_client = MongoClient(MONGO_URI)

db = mongo_client.wikipulse
collection = db.edits

# Indexes for Analytics
collection.create_index([("timestamp", -1)])
collection.create_index([("user", 1)])
collection.create_index([("title", 1)])

def process_message(message):
    try:
        data = json.loads(message.data.decode("utf-8"))

        # Basic validation
        
        meta = data.get("meta", {})
        event_id = meta.get("id") or data.get("id") # Fallback to top-level just in case

        if not event_id or not data.get("timestamp"):
            logger.warning("Invalid event skipped: Missing ID or Timestamp.")
            message.ack()
            return
        
        # Optional bot filtering
        if data.get("bot"):
            message.ack()
            return

        # Standardized document
        processed_doc = {
            "event_id": event_id,
            "title": data.get("title"),
            "user": data.get("user"),
            "timestamp": data.get("timestamp"),
            "timestamp_utc": datetime.fromtimestamp(data.get("timestamp"), tz=timezone.utc),
            "wiki": data.get("wiki"),
            "is_bot": data.get("bot", False),
            "ingested_at": data.get("_ingested_at"),
            "processed_at": datetime.now(timezone.utc)

        }

        # Deduplication via upsert
        collection.update_one(
            {"event_id": processed_doc["event_id"]},
            {"$set": processed_doc},
            upsert=True
        )

        message.ack()

    except Exception as e:
        logger.error(f"Processing error: {e}")
        message.nack()

if __name__ == "__main__":

    subscriber = pubsub_v1.SubscriberClient()

    sub_path = subscriber.subscription_path(
        GCP_PROJECT_ID,
        SUBSCRIPTION_ID
    )

    flow_control = pubsub_v1.types.FlowControl(
        max_messages=50
    )

    logger.info(f"Listening on {sub_path}...")

    streaming_pull_future = subscriber.subscribe(
        sub_path,
        callback=process_message,
        flow_control=flow_control
    )

    try:
        streaming_pull_future.result()

    except KeyboardInterrupt:
        logger.info("Shutting down processor...")
        streaming_pull_future.cancel()