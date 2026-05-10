# WikiPulse: Real-Time Wikipedia Intelligence


Project OverviewWikiPulse is a real-time data pipeline designed to detect global events as they happen. Instead of waiting for news reports, this system listens to live "edit events" from Wikipedia. By processing these streams, we can identify trending topics, breaking news, and cultural shifts in sub-second time.


---


# Architecture 


The project follows a decoupled streaming architecture:


-Ingestion Layer: A Python script connecting to Wikimedia's SSE stream.


-Messaging Layer: Google Cloud Pub/Sub handles the data flow and ensures reliability.


-Processing Layer: A Python consumer that cleans the data and filters out bots.


-Storage Layer: MongoDB Atlas stores the processed events for analysis.


----


# How to Run the Project


# 1. Prerequisites

   
-Python 3.10+
-A Google Cloud Project (with Pub/Sub enabled)
-A MongoDB Atlas Cluster
-A .env file 


# 2. setup
Create and activate virtual environment


python -m venv venv
source venv/bin/activate  # Or .\venv\Scripts\activate on Windows


Install dependencies


pip install -r requirements.txt


do the following


-Create a Google Cloud Service Account and download the JSON key.


-Create a .env file based on the provided .env.example.


-Set the GOOGLE_APPLICATION_CREDENTIALS variable to your key's filename.


# 3. Execution


-Start Ingestor: python ingestion.py (Starts sending data to Pub/Sub).


-Start Processor: python processing.py (Pulls data from Pub/Sub to MongoDB).


---


# Analytical Insights


With the data in MongoDB, I ran queries to find:


-Trending Pages: The top 5 articles receiving the most edits.


-Bot vs. Human Activity: Measuring the volume of automated edits.


-Global Activity: Distribution of edits across different language wikis.


---


Lessons Learned


-Handling Streams: I learned how to manage persistent connections using requests and how to handle "heartbeat" signals in SSE.


-Decoupling: Using a Message Broker (Pub/Sub) showed me how to keep the ingestor running even if the database is busy.


-Data Cleaning: I practiced filtering "noisy" data (bots) before it hits the storage layer to save costs and space.
