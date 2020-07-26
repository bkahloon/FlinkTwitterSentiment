# FlinkTwitterSentiment

Project to test out hybrid cloud deployments. The project will read tweets from twitter in real time by utilizing Kinesis Data Analytics and perform sentiment analysis on entitites in those tweets using GCP's Natural Language API. These results will then be transformed and written to Kafka (AWS MSK). A Cloud Dataflow application will then read this Kafka topic and perform some windowed aggregations to calculate the sentiment towards a specific entity in the past 1 minute. These results will then be written to Pub/Sub, Big Query and back to MSK in Avro format. We will then use a KSQL cluster being ran on GCP's Kubernetes Engine.

## Proposed Architecture

![GCP-project](https://user-images.githubusercontent.com/42993708/77286266-8034e380-6c98-11ea-89d6-62817ff9a391.png)
