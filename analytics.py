
#Each function corresponds to a specific analytical query against the MongoDB collection where processed Wikipedia edit events are stored.
import logging
import os
from datetime import datetime, timezone, timedelta

from pymongo import MongoClient

# Configuration

MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://localhost:27017"
)

MONGO_DB = os.environ.get(
    "MONGO_DB",
    "wikipulse"
)

MONGO_COLLECTION = os.environ.get(
    "MONGO_COLLECTION",
    "wiki_events"
)

# Logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger("analytics")

# MongoDB Connection

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000
)

collection = client[MONGO_DB][MONGO_COLLECTION]

# Indexes

# Unique event ID index (deduplication)
collection.create_index(
    [("event_id", 1)],
    unique=True
)

# Time-based analytics
collection.create_index(
    [("timestamp_utc", -1)]
)

# User activity queries
collection.create_index(
    [("user", 1), ("timestamp_utc", -1)]
)

# Trending pages queries
collection.create_index(
    [("title", 1), ("timestamp_utc", -1)]
)

# Bot vs human queries
collection.create_index(
    [("wiki", 1), ("is_bot", 1)]
)

# Schema Reference

"""
Document Structure Example:

{
  "_id": ObjectId,

  "event_id": "12345678",

  "wiki": "enwiki",
  "title": "Artificial intelligence",
  "event_type": "edit",
  "namespace": 0,

  "timestamp_utc": ISODate(...),

  "year": 2026,
  "month": 5,
  "day": 2,
  "hour": 14,

  "user": "SomeEditor",
  "is_bot": false,
  "is_minor": false,

  "comment": "Fixed citation",

  "edit_delta": 150,

  "revision_old": 987654320,
  "revision_new": 987654321,

  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",

  "ingested_at": "2026-05-02T14:00:00+00:00",
  "processed_at": "2026-05-02T14:00:01+00:00"
}
"""

# QUERY 1 — Top Edited Pages

def top_edited_pages(
    hours: int = 1,
    limit: int = 10
) -> list:
    """
    Returns the most edited Wikipedia pages
    within a given time window.
    """

    logger.info("Running top edited pages query...")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    pipeline = [
        {
            "$match": {
                "timestamp_utc": {"$gte": since},
                "is_bot": False,
                "namespace": 0
            }
        },
        {
            "$group": {
                "_id": "$title",
                "edit_count": {"$sum": 1},
                "wiki": {"$first": "$wiki"},
                "last_edit": {"$max": "$timestamp_utc"}
            }
        },
        {
            "$sort": {
                "edit_count": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "title": "$_id",
                "wiki": 1,
                "edit_count": 1,
                "last_edit": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    return results if results else []

# QUERY 2 — Most Active Users

def most_active_users(
    hours: int = 1,
    limit: int = 10,
    exclude_bots: bool = True
) -> list:
    """
    Returns the most active Wikipedia editors.
    """

    logger.info("Running most active users query...")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    match_stage = {
        "timestamp_utc": {"$gte": since}
    }

    if exclude_bots:
        match_stage["is_bot"] = False

    pipeline = [
        {
            "$match": match_stage
        },
        {
            "$group": {
                "_id": "$user",
                "edit_count": {"$sum": 1},
                "pages_edited": {"$addToSet": "$title"},
                "wikis": {"$addToSet": "$wiki"}
            }
        },
        {
            "$addFields": {
                "unique_pages": {
                    "$size": "$pages_edited"
                }
            }
        },
        {
            "$sort": {
                "edit_count": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "user": "$_id",
                "edit_count": 1,
                "unique_pages": 1,
                "wikis": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    return results if results else []

# QUERY 3 — Bot vs Human Ratio

def bot_vs_human_ratio(
    hours: int = 24
) -> list:
    """
    Returns bot vs human edit distribution
    grouped by wiki.
    """

    logger.info("Running bot vs human ratio query...")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    pipeline = [
        {
            "$match": {
                "timestamp_utc": {"$gte": since}
            }
        },
        {
            "$group": {
                "_id": {
                    "wiki": "$wiki",
                    "is_bot": "$is_bot"
                },
                "count": {"$sum": 1}
            }
        },
        {
            "$group": {
                "_id": "$_id.wiki",
                "totals": {
                    "$push": {
                        "is_bot": "$_id.is_bot",
                        "count": "$count"
                    }
                },
                "total_edits": {
                    "$sum": "$count"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "wiki": "$_id",
                "total_edits": 1,
                "breakdown": "$totals"
            }
        },
        {
            "$sort": {
                "total_edits": -1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    return results if results else []


# QUERY 4 — Edit Frequency by Hour

def edit_frequency_by_hour(
    hours: int = 24,
    wiki: str = "enwiki"
) -> list:
    """
    Detects spikes in edit activity
    using hourly time buckets.
    """

    logger.info("Running edit frequency query...")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    pipeline = [
        {
            "$match": {
                "timestamp_utc": {"$gte": since},
                "wiki": wiki
            }
        },
        {
            "$group": {
                "_id": {
                    "year": "$year",
                    "month": "$month",
                    "day": "$day",
                    "hour": "$hour"
                },
                "edit_count": {
                    "$sum": 1
                },
                "human_edits": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$is_bot", False]},
                            1,
                            0
                        ]
                    }
                },
                "bot_edits": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$is_bot", True]},
                            1,
                            0
                        ]
                    }
                }
            }
        },
        {
            "$sort": {
                "_id.year": 1,
                "_id.month": 1,
                "_id.day": 1,
                "_id.hour": 1
            }
        },
        {
            "$project": {
                "_id": 0,
                "bucket": "$_id",
                "edit_count": 1,
                "human_edits": 1,
                "bot_edits": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    return results if results else []

# QUERY 5 — Largest Edits

def largest_edits(
    hours: int = 1,
    limit: int = 10
) -> list:
    """
    Returns edits with the largest byte changes.
    Useful for detecting major content rewrites.
    """

    logger.info("Running largest edits query...")

    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    pipeline = [
        {
            "$match": {
                "timestamp_utc": {"$gte": since},
                "edit_delta": {"$ne": None},
                "is_bot": False
            }
        },
        {
            "$addFields": {
                "abs_delta": {
                    "$abs": "$edit_delta"
                }
            }
        },
        {
            "$sort": {
                "abs_delta": -1
            }
        },
        {
            "$limit": limit
        },
        {
            "$project": {
                "_id": 0,
                "title": 1,
                "user": 1,
                "wiki": 1,
                "edit_delta": 1,
                "timestamp_utc": 1,
                "comment": 1
            }
        }
    ]

    results = list(collection.aggregate(pipeline))

    return results if results else []

# Runner
if __name__ == "__main__":

    import json

    def pretty(label, data):

        print("\n" + "=" * 60)
        print(f"  {label}")
        print("=" * 60)

        print(
            json.dumps(
                data,
                indent=2,
                default=str
            )
        )

    pretty(
        "Top Edited Pages (Last 1 Hour)",
        top_edited_pages(hours=1)
    )

    pretty(
        "Most Active Users (Last 1 Hour)",
        most_active_users(hours=1)
    )

    pretty(
        "Bot vs Human Ratio (Last 24 Hours)",
        bot_vs_human_ratio(hours=24)
    )

    pretty(
        "Hourly Edit Frequency - enwiki",
        edit_frequency_by_hour(
            hours=24,
            wiki="enwiki"
        )
    )

    pretty(
        "Largest Edits by Byte Delta",
        largest_edits(hours=1)
    )