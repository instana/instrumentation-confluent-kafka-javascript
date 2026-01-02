# OpenTelemetry `confluent-kafka-javascript` Instrumentation for Node.js

This instrumentation is a **fully-featured** and fully tested Opentelemetry instrumentation for the [`@confluentinc/kafka-javascript`](https://www.npmjs.com/package/@confluentinc/kafka-javascript) package.

```bash
npm install @instana/instrumentation-confluent-kafka-javascript
```

The Opentelemetry instrumentation was originally started by [Drazke](https://github.com/Drazke) [here](https://github.com/Drazke/instrumentation-confluent-kafka-javascript). We will merge them together as soon as possible.


## Test

```bash
# Kafka needs to be running locally on port 9092
npm run test
```