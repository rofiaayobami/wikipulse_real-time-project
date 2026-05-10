
import json
import logging
import os
import time
from datetime import datetime, timezone
import requests
from google.cloud import pubsub_v1
from google.api_core.exceptions import GoogleAPICallError
from dotenv import load_dotenv

load_dotenv()

key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if key_path:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Configuration details for the ingestion layer
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
PUBSUB_TOPIC_ID = os.environ.get("PUBSUB_TOPIC_ID", "wikipulse-raw-events")

# Validation: Essential fields the downstream processor expects
REQUIRED_FIELDS = {"meta", "type", "wiki", "timestamp"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ingestor")

class WikiIngestor:
    def __init__(self):  
        # Messaging Layer: Decoupled via Pub/Sub with high-throughput batching
        self.publisher = pubsub_v1.PublisherClient(
            batch_settings=pubsub_v1.types.BatchSettings(
                max_messages=100, 
                max_latency=0.1
            )
        )
        self.topic_path = self.publisher.topic_path(GCP_PROJECT_ID, PUBSUB_TOPIC_ID)

    def validate_initial(self, event: dict) -> bool:
        """Requirement: Perform initial validation and parsing."""
        # Ensure it's a dictionary and has the bare-minimum fields
        if not isinstance(event, dict):
            return False
        return REQUIRED_FIELDS.issubset(event.keys())

    def publish(self, event):
        # Mark exactly when we received the event
        event["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        
        data = json.dumps(event).encode("utf-8")

        logger.info(f"Publishing event from {event.get('wiki')}")
        
        # Publish with attributes for server-side filtering (e.g., skip bots in the cloud)
        future = self.publisher.publish(
            self.topic_path, 
            data, 
            is_bot=str(event.get("bot", False)).lower(),
            wiki=str(event.get("wiki", ""))
        )
        future.add_done_callback(self._publish_callback)

    @staticmethod
    def _publish_callback(future):
        try:
            future.result() # Re-introduced check for GoogleAPICallError
        except GoogleAPICallError as e:
            logger.error(f"Messaging Layer Error: {e}")

    def start_streaming(self):
        """Continuously receive JSON events with resilience."""
        logger.info(f"Connecting to WikiPulse Stream: {STREAM_URL}")
        # Custom User-Agent to comply with Wikimedia's guidelines
        headers = {
            'User-Agent': 'WikiPulseBot/1.0 (rofiaayobami@gmail.com)'
        }

        while True:  # Resilience loop to handle network drops
            try:
                # Use stream=True to process events as they arrive (Low-latency)
                with requests.get(STREAM_URL, headers=headers, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    logger.info("Stream Connected. Receiving events...")
                    
                    for line in r.iter_lines(decode_unicode=True):
                        # SSE lines start with 'data: '
                        if line and line.startswith("data:"):
                            try:
                                # Parsing the JSON payload 
                                payload = line.removeprefix("data:").strip()
                                event = json.loads(payload)
                                # Initial Validation 
                                if self.validate_initial(event) and event.get("type") in ("edit", "new"):
                                    self.publish(event)
                                    
                            except json.JSONDecodeError:
                                # Handles empty 'heartbeat' lines from Wikimedia
                                continue
            
            except requests.exceptions.RequestException as e: #handles major network-related exceptions
                logger.error(f"Network Error: {e}")
                time.sleep(5)
                
            except Exception as e:
                logger.exception(f"Unexpected Error: {e}. Reconnecting in 5s...")
                time.sleep(5)  # Backoff before retrying connection 

            
        

if __name__ == "__main__":
    WikiIngestor().start_streaming()
