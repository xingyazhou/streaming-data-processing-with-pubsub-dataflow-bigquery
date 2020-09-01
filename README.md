# streaming-data-processing-with-pubsub-dataflow-bigquery

## Introduction

[Google Cloud Platform training project:Streaming Data Processing](https://github.com/GoogleCloudPlatform/training-data-analyst/tree/master/courses/streaming)
At the time Google designed this project, streaming pipelines are not available in the DataFlow **python** SDK. So the streaming labs are written in Java.

This project is to rewrite the above streaming pipelines in Python (Apache Beam Python):
1. Use Dataflow to collect traffic events from simulated traffic sensor data through Google PubSub
2. Process the simulated traffice sensor data
3. Write the data into BigQuery for further analysis




