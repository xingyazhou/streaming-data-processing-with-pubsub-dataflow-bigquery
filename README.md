# streaming-data-processing-with-pubsub-dataflow-bigquery

## Introduction

This project is to rewrite the streaming pipelines in Python (Apache Beam Python):
1. Use Dataflow to collect traffic events from simulated traffic sensor data through Google PubSub
2. Process the simulated traffice sensor data in to average
3. Write the data into BigQuery for futher analysis

At the time the original project was designed for GCP Data Engineering traing course at Cousera, streaming pipelines are not available in the DataFlow Python SDK. So the streaming labs are written in Java.



