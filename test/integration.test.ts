import * as assert from "assert";
import {
  ConfluentKafkaInstrumentation,
} from "../lib";
import { ReadableSpan } from "@opentelemetry/sdk-trace-base";
import {
  propagation,
  context,
  SpanKind,
  SpanStatusCode,
  diag,
} from "@opentelemetry/api";
import {
  ATTR_MESSAGING_SYSTEM,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_KAFKA_OFFSET,
  ATTR_MESSAGING_OPERATION_NAME,
  MESSAGING_SYSTEM_VALUE_KAFKA,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
} from "@opentelemetry/semantic-conventions/incubating";
import {
  getTestSpans,
  initMeterProvider,
  registerInstrumentationTesting,
  TestMetricReader,
} from "@opentelemetry/contrib-test-utils";
import { W3CBaggagePropagator, CompositePropagator, W3CTraceContextPropagator } from "@opentelemetry/core";

// Set global propagator only if not already set
try {
  propagation.setGlobalPropagator(
    new CompositePropagator({
      propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()],
    })
  );
} catch (e) {
  // Already set, ignore
}

const instrumentation = registerInstrumentationTesting(
  new ConfluentKafkaInstrumentation() as any
);

import * as ConfluentKafka from "@confluentinc/kafka-javascript";

describe("integration test with Callback API (rdkafka)", () => {
  const BROKER = "localhost:9092";
  const TOPIC = "integration-test-topic";
  const GROUP_ID = `integration-test-group-${Date.now()}`;

  let producer: ConfluentKafka.Producer;
  let consumer: ConfluentKafka.KafkaConsumer;
  let metricReader: TestMetricReader;
  let pollInterval: NodeJS.Timeout;

  before(async function () {
    this.timeout(15000);
    metricReader = initMeterProvider(instrumentation);

    const producerConfig = {
      "bootstrap.servers": BROKER,
      "client.id": "integration-test-producer",
    };

    const consumerConfig = {
      "bootstrap.servers": BROKER,
      "group.id": GROUP_ID,
      "client.id": "integration-test-consumer",
      "auto.offset.reset": "earliest",
    };

    producer = new ConfluentKafka.Producer(producerConfig);
    consumer = new ConfluentKafka.KafkaConsumer(consumerConfig);

    await new Promise<void>((resolve, reject) => {
      producer.on("ready", () => {
        consumer.on("ready", () => {
          consumer.subscribe([TOPIC]);
          consumer.consume();
          pollInterval = setInterval(() => {
            producer.poll();
          }, 100);
          resolve();
        });
        consumer.on("connection.failure", reject);
        consumer.connect();
      });
      producer.on("connection.failure", reject);
      producer.connect();
    });
  });

  after(async function () {
    this.timeout(5000);
    if (pollInterval) {
      clearInterval(pollInterval);
    }
    try {
      await producer.disconnect();
    } catch (e) {
      // ignore
    }
    try {
      await consumer.disconnect();
    } catch (e) {
      // ignore
    }
  });

  it("should create producer span when producing message", async function () {
    this.timeout(20000);

    const testKey = `test-key-1-${Date.now()}`;
    const testValue = "test-value-1";

    producer.produce(
      TOPIC,
      null,
      Buffer.from(testValue, "utf8"),
      Buffer.from(testKey, "utf8"),
      Date.now()
    );

    await new Promise<void>((resolve) => {
      producer.flush(10000, () => {
        resolve();
      });
    });

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const spans = getTestSpans();
    const produceSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "produce" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    assert.strictEqual(
      produceSpans.length,
      1,
      `Should create one produce span, found ${produceSpans.length}`
    );

    const span = produceSpans[0] as ReadableSpan;
    assert.strictEqual(span.kind, SpanKind.PRODUCER);
    assert.strictEqual(span.name, TOPIC);
    assert.strictEqual(span.status.code, SpanStatusCode.UNSET);
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_SYSTEM],
      MESSAGING_SYSTEM_VALUE_KAFKA
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_NAME],
      TOPIC
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_NAME],
      "produce"
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_TYPE],
      MESSAGING_OPERATION_TYPE_VALUE_SEND
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY],
      testKey
    );
  });

  it("should create consumer span when consuming message", async function () {
    this.timeout(15000);

    const testKey = `test-key-2-${Date.now()}`;
    const testValue = "test-value-2";

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      topic: string;
      partition: number;
      offset: string;
      key: Buffer | null;
      value: Buffer | null;
    }>((resolve, reject) => {
      const messageHandler = (message: any) => {
        if (messageReceived) return;
        const messageKey = message.key?.toString();
        if (message.topic === TOPIC && messageKey === testKey) {
          messageReceived = true;
          clearTimeout(timeout);
          consumer.removeListener("data", messageHandler);
          resolve({
            topic: message.topic,
            partition: message.partition,
            offset: message.offset.toString(),
            key: message.key,
            value: message.value,
          });
        }
      };

      const timeout = setTimeout(() => {
        consumer.removeListener("data", messageHandler);
        reject(new Error("Timeout waiting for message"));
      }, 10000);

      consumer.on("data", messageHandler);
    });

    producer.produce(
      TOPIC,
      null,
      Buffer.from(testValue, "utf8"),
      Buffer.from(testKey, "utf8"),
      Date.now()
    );

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;

    assert.strictEqual(received.topic, TOPIC);
    assert.strictEqual(received.key?.toString(), testKey);
    assert.strictEqual(received.value?.toString(), testValue);

    const spans = getTestSpans();
    const allReceiveSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "received" &&
        span.kind === SpanKind.CONSUMER &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    const receiveSpans = Array.from(
      new Map(
        allReceiveSpans.map((span) => [
          `${span.attributes[ATTR_MESSAGING_DESTINATION_NAME]}-${span.attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID]}-${span.attributes[ATTR_MESSAGING_KAFKA_OFFSET]}`,
          span
        ])
      ).values()
    );

    assert.strictEqual(
      receiveSpans.length,
      1,
      `Should create one receive span, found ${receiveSpans.length}`
    );

    const span = receiveSpans[0] as ReadableSpan;
    assert.strictEqual(span.kind, SpanKind.CONSUMER);
    assert.strictEqual(span.name, TOPIC);
    assert.strictEqual(span.status.code, SpanStatusCode.UNSET);
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_SYSTEM],
      MESSAGING_SYSTEM_VALUE_KAFKA
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_NAME],
      TOPIC
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_NAME],
      "received"
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_TYPE],
      MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY],
      testKey
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_OFFSET],
      received.offset
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID],
      received.partition.toString()
    );
  });

  it("should propagate context from producer to consumer", async function () {
    this.timeout(20000);

    const testKey = `test-key-3-${Date.now()}`;
    const testValue = "test-value-3";

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      message: any;
    }>((resolve, reject) => {
      const messageHandler = (message: any) => {
        if (messageReceived) return;
        const messageKey = message.key?.toString();
        if (message.topic === TOPIC && messageKey === testKey) {
          messageReceived = true;
          clearTimeout(timeout);
          consumer.removeListener("data", messageHandler);
          
          resolve({
            message,
          });
        }
      };

      const timeout = setTimeout(() => {
        consumer.removeListener("data", messageHandler);
        reject(new Error("Timeout waiting for message"));
      }, 15000);

      consumer.on("data", messageHandler);
    });

    const headers: any[] = [];
    producer.produce(
      TOPIC,
      null,
      Buffer.from(testValue, "utf8"),
      Buffer.from(testKey, "utf8"),
      Date.now(),
      undefined,
      headers
    );
    

    await new Promise<void>((resolve) => {
      producer.flush(10000, () => {
        resolve();
      });
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;

    await new Promise((resolve) => setTimeout(resolve, 500));

    const spans = getTestSpans();
    const produceSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "produce" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    const allReceiveSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "received" &&
        span.kind === SpanKind.CONSUMER &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey &&
        span.attributes[ATTR_MESSAGING_KAFKA_OFFSET] === received.message?.offset?.toString()
    );
    
    const receiveSpans = Array.from(
      new Map(
        allReceiveSpans.map((span) => [
          `${span.attributes[ATTR_MESSAGING_DESTINATION_NAME]}-${span.attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID]}-${span.attributes[ATTR_MESSAGING_KAFKA_OFFSET]}`,
          span
        ])
      ).values()
    );

    assert.strictEqual(
      produceSpans.length,
      1,
      `Expected 1 produce span, found ${produceSpans.length}. Spans: ${JSON.stringify(produceSpans.map(s => ({ name: s.name, key: s.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] })))}`
    );
    assert.strictEqual(
      receiveSpans.length,
      1,
      `Expected 1 receive span, found ${receiveSpans.length}. Spans: ${JSON.stringify(receiveSpans.map(s => ({ name: s.name, key: s.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY], offset: s.attributes[ATTR_MESSAGING_KAFKA_OFFSET] })))}`
    );

    const produceSpan = produceSpans[0] as ReadableSpan;
    const receiveSpan = receiveSpans[0] as ReadableSpan;

    assert.strictEqual(
      receiveSpan.spanContext().traceId,
      produceSpan.spanContext().traceId,
      "Consumer span should have same traceId as producer span"
    );
  });

  it("should preserve custom headers and not lose them", async function () {
    this.timeout(20000);

    const testKey = `test-key-4-${Date.now()}`;
    const testValue = "test-value-4";
    const customHeaderKey = "custom-header";
    const customHeaderValue = "custom-value-123";

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      message: any;
    }>((resolve, reject) => {
      const messageHandler = (message: any) => {
        if (messageReceived) return;
        const messageKey = message.key?.toString();
        if (message.topic === TOPIC && messageKey === testKey) {
          messageReceived = true;
          clearTimeout(timeout);
          consumer.removeListener("data", messageHandler);
          
          resolve({
            message,
          });
        }
      };

      const timeout = setTimeout(() => {
        consumer.removeListener("data", messageHandler);
        reject(new Error("Timeout waiting for message"));
      }, 15000);

      consumer.on("data", messageHandler);
    });

    const headers: any[] = [
      { [customHeaderKey]: Buffer.from(customHeaderValue, "utf8") }
    ];
    producer.produce(
      TOPIC,
      null,
      Buffer.from(testValue, "utf8"),
      Buffer.from(testKey, "utf8"),
      Date.now(),
      undefined,
      headers
    );

    await new Promise<void>((resolve) => {
      producer.flush(10000, () => {
        resolve();
      });
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;

    await new Promise((resolve) => setTimeout(resolve, 500));

    // Check that custom header is preserved
    assert.ok(received.message.headers, "Message should have headers");
    const customHeader = received.message.headers.find((h: any) => {
      return Object.keys(h).some(key => key === customHeaderKey);
    });
    assert.ok(customHeader, `Custom header '${customHeaderKey}' should be present`);
    assert.strictEqual(
      customHeader[customHeaderKey]?.toString("utf8"),
      customHeaderValue,
      `Custom header value should match. Expected: ${customHeaderValue}, Got: ${customHeader[customHeaderKey]?.toString("utf8")}`
    );

    // Check that traceparent header is also present (OTel propagation)
    const traceparentHeader = received.message.headers.find((h: any) => {
      return Object.keys(h).some(key => key === "traceparent");
    });
    assert.ok(traceparentHeader, "traceparent header should be present for context propagation");
    assert.ok(traceparentHeader.traceparent, "traceparent header should have a value");

    // Verify that both headers coexist
    assert.strictEqual(
      received.message.headers.length,
      2,
      `Should have 2 headers (custom + traceparent), found ${received.message.headers.length}`
    );
  });
});

describe("integration test with Promise API (KafkaJS)", () => {
  const BROKER = "localhost:9092";
  const TOPIC = `integration-test-topic-promise-${Date.now()}`;
  const GROUP_ID = `integration-test-group-promise-${Date.now()}`;

  let producer: any;
  let consumer: any;
  let metricReader: TestMetricReader;

  before(async function () {
    this.timeout(15000);
    metricReader = initMeterProvider(instrumentation);

    const { Kafka } = (ConfluentKafka as any).KafkaJS;
    const kafka = new Kafka();

    producer = kafka.producer({
      "bootstrap.servers": BROKER,
      "client.id": "integration-test-producer-promise",
    });

    await producer.connect();
  });

  after(async function () {
    this.timeout(5000);
    try {
      await producer.disconnect();
    } catch (e) {
      // ignore
    }
    if (consumer) {
      try {
        await consumer.disconnect();
      } catch (e) {
        // ignore
      }
    }
  });

  it("should create producer span when sending message with send()", async function () {
    this.timeout(20000);

    const testKey = `test-key-promise-1-${Date.now()}`;
    const testValue = "test-value-promise-1";

    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: testKey,
          value: testValue,
        },
      ],
    });

    await producer.flush({ timeout: 10000 });
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const spans = getTestSpans();
    const produceSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "send" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    const produceSpansFromCallback = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "produce" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    const deliveryReportSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "delivery-report" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    assert.strictEqual(
      produceSpans.length,
      1,
      `Should create one send span, found ${produceSpans.length}`
    );
    
    assert.strictEqual(
      produceSpansFromCallback.length,
      0,
      `Expected 0 produce spans (Callback API should not create span when called from Promise API), found ${produceSpansFromCallback.length}`
    );
    
    assert.strictEqual(
      deliveryReportSpans.length,
      0,
      `Expected 0 delivery-report spans (instrumentation should not automatically register delivery-report listener), found ${deliveryReportSpans.length}`
    );
    
    const allProducerSpans = spans.filter(
      (span) =>
        span.kind === SpanKind.PRODUCER &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    assert.strictEqual(
      allProducerSpans.length,
      1,
      `Expected 1 producer span total (send only, no delivery-report), found ${allProducerSpans.length}. Spans: ${JSON.stringify(allProducerSpans.map(s => ({ name: s.name, operation: s.attributes[ATTR_MESSAGING_OPERATION_NAME] })))}`
    );

    const span = produceSpans[0] as ReadableSpan;
    assert.strictEqual(span.kind, SpanKind.PRODUCER);
    assert.strictEqual(span.name, TOPIC);
    assert.strictEqual(span.status.code, SpanStatusCode.UNSET);
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_SYSTEM],
      MESSAGING_SYSTEM_VALUE_KAFKA
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_NAME],
      TOPIC
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_NAME],
      "send"
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_TYPE],
      MESSAGING_OPERATION_TYPE_VALUE_SEND
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY],
      testKey
    );
  });

  it("should create consumer span when consuming message with run()", async function () {
    this.timeout(20000);

    const testKey = `test-key-promise-2-${Date.now()}`;
    const testValue = "test-value-promise-2";
    const uniqueGroupId = `${GROUP_ID}-test-2-${Date.now()}`;

    // First, send a message to create the topic
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: `dummy-${Date.now()}`,
          value: "dummy",
        },
      ],
    });
    
    // Wait for topic to be created
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Create a new consumer for this test
    const { Kafka } = (ConfluentKafka as any).KafkaJS;
    const kafka = new Kafka();
    const testConsumer = kafka.consumer({
      "bootstrap.servers": BROKER,
      "group.id": uniqueGroupId,
      "client.id": "integration-test-consumer-promise-2",
      "auto.offset.reset": "earliest",
    });

    await testConsumer.connect();
    await testConsumer.subscribe({ topics: [TOPIC] });

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      topic: string;
      partition: number;
      offset: string;
      key: Buffer | null;
      value: Buffer | null;
    }>((resolve, reject) => {
      const timeout = setTimeout(() => {
        testConsumer.disconnect().catch(() => {});
        reject(new Error("Timeout waiting for message"));
      }, 10000);

      // Start consumer.run() - it runs continuously
      // Don't await it, just start it and let it run
      testConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (messageReceived) return;
          const messageKey = message.key?.toString();
          if (topic === TOPIC && messageKey === testKey) {
            messageReceived = true;
            clearTimeout(timeout);
            // Wait a bit for span to be ended before resolving
            await new Promise((resolve) => setTimeout(resolve, 100));
            resolve({
              topic,
              partition,
              offset: message.offset,
              key: message.key,
              value: message.value,
            });
          }
        },
      }).catch((err: any) => {
        if (!messageReceived) {
          clearTimeout(timeout);
          reject(err);
        }
      });
    });

    // Wait a bit for consumer to be ready
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: testKey,
          value: testValue,
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;

    // Wait a bit for span to be ended
    await new Promise((resolve) => setTimeout(resolve, 500));
    
    // Disconnect consumer to stop the continuous run() loop
    try {
      await testConsumer.disconnect();
    } catch (e) {
      // ignore
    }

    assert.strictEqual(received.topic, TOPIC);
    assert.strictEqual(received.key?.toString(), testKey);
    assert.strictEqual(received.value?.toString(), testValue);

    const spans = getTestSpans();
    const allReceiveSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "received" &&
        span.kind === SpanKind.CONSUMER &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );

    const receiveSpans = Array.from(
      new Map(
        allReceiveSpans.map((span) => [
          `${span.attributes[ATTR_MESSAGING_DESTINATION_NAME]}-${span.attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID]}-${span.attributes[ATTR_MESSAGING_KAFKA_OFFSET]}`,
          span,
        ])
      ).values()
    );

    assert.strictEqual(
      receiveSpans.length,
      1,
      `Should create one receive span, found ${receiveSpans.length}`
    );

    const span = receiveSpans[0] as ReadableSpan;
    assert.strictEqual(span.kind, SpanKind.CONSUMER);
    assert.strictEqual(span.name, TOPIC);
    assert.strictEqual(span.status.code, SpanStatusCode.UNSET);
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_SYSTEM],
      MESSAGING_SYSTEM_VALUE_KAFKA
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_NAME],
      TOPIC
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_NAME],
      "received"
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_OPERATION_TYPE],
      MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY],
      testKey
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_KAFKA_OFFSET],
      received.offset
    );
    assert.strictEqual(
      span.attributes[ATTR_MESSAGING_DESTINATION_PARTITION_ID],
      received.partition.toString()
    );
  });

  it("should propagate context from producer to consumer with Promise API", async function () {
    this.timeout(20000);

    const testKey = `test-key-promise-3-${Date.now()}`;
    const testValue = "test-value-promise-3";
    const uniqueGroupId = `${GROUP_ID}-test-3-${Date.now()}`;

    // First, send a dummy message to create the topic
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: `dummy-${Date.now()}`,
          value: "dummy",
        },
      ],
    });
    
    // Wait for topic to be created
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Create a new consumer for this test
    const { Kafka } = (ConfluentKafka as any).KafkaJS;
    const kafka = new Kafka();
    const testConsumer = kafka.consumer({
      "bootstrap.servers": BROKER,
      "group.id": uniqueGroupId,
      "client.id": "integration-test-consumer-promise-3",
      "auto.offset.reset": "earliest",
    });

    await testConsumer.connect();
    await testConsumer.subscribe({ topics: [TOPIC] });

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      topic: string;
      partition: number;
      offset: string;
    }>((resolve, reject) => {
      const timeout = setTimeout(() => {
        testConsumer.disconnect().catch(() => {});
        reject(new Error("Timeout waiting for message"));
      }, 15000);

      // Start consumer.run() - it runs continuously
      testConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (messageReceived) return;
          const messageKey = message.key?.toString();
          if (topic === TOPIC && messageKey === testKey) {
            messageReceived = true;
            clearTimeout(timeout);
            // Wait a bit for span to be ended before resolving
            await new Promise((resolve) => setTimeout(resolve, 100));
            resolve({
              topic,
              partition,
              offset: message.offset,
            });
          }
        },
      }).catch((err: any) => {
        if (!messageReceived) {
          clearTimeout(timeout);
          reject(err);
        }
      });
    });

    // Wait longer for consumer to be ready and subscribed
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Send the test message
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: testKey,
          value: testValue,
        },
      ],
    });
    
    // Wait a bit for message to be sent
    await new Promise((resolve) => setTimeout(resolve, 500));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;
    
    // Wait a bit for span to be ended
    await new Promise((resolve) => setTimeout(resolve, 500));
    
    // Disconnect consumer to stop the continuous run() loop
    try {
      await testConsumer.disconnect();
    } catch (e) {
      // ignore
    }

    const spans = getTestSpans();
    const produceSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "send" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    // Check that we don't have duplicate "produce" spans (from Callback API)
    const produceSpansFromCallback = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "produce" &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey
    );
    
    assert.strictEqual(
      produceSpans.length,
      1,
      `Expected 1 send span, found ${produceSpans.length}`
    );
    
    assert.strictEqual(
      produceSpansFromCallback.length,
      0,
      `Expected 0 produce spans (Callback API should not create span when called from Promise API), found ${produceSpansFromCallback.length}`
    );

    const produceSpan = produceSpans[0] as ReadableSpan;
    const expectedTraceId = produceSpan.spanContext().traceId;

    // Find the consumer span with the correct trace ID
    const receiveSpans = spans.filter(
      (span) =>
        span.attributes[ATTR_MESSAGING_OPERATION_NAME] === "received" &&
        span.kind === SpanKind.CONSUMER &&
        span.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey &&
        span.spanContext().traceId === expectedTraceId
    );

    assert.strictEqual(
      receiveSpans.length,
      1,
      `Expected 1 receive span with traceId ${expectedTraceId}, found ${receiveSpans.length}. All consumer spans: ${JSON.stringify(spans.filter(s => s.kind === SpanKind.CONSUMER && s.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] === testKey).map(s => ({ traceId: s.spanContext().traceId, key: s.attributes[ATTR_MESSAGING_KAFKA_MESSAGE_KEY] })), null, 2)}`
    );

    const receiveSpan = receiveSpans[0] as ReadableSpan;

    assert.strictEqual(
      receiveSpan.spanContext().traceId,
      expectedTraceId,
      "Consumer span should have same traceId as producer span"
    );
  });

  it("should preserve custom headers with Promise API", async function () {
    this.timeout(20000);

    const testKey = `test-key-promise-4-${Date.now()}`;
    const testValue = "test-value-promise-4";
    const customHeaderKey = "custom-header";
    const customHeaderValue = "custom-value-123";
    const uniqueGroupId = `${GROUP_ID}-test-4-${Date.now()}`;

    // First, send a message to create the topic
    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: `dummy-${Date.now()}`,
          value: "dummy",
        },
      ],
    });
    
    // Wait for topic to be created
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Create a new consumer for this test
    const { Kafka } = (ConfluentKafka as any).KafkaJS;
    const kafka = new Kafka();
    const testConsumer = kafka.consumer({
      "bootstrap.servers": BROKER,
      "group.id": uniqueGroupId,
      "client.id": "integration-test-consumer-promise-4",
      "auto.offset.reset": "earliest",
    });

    await testConsumer.connect();
    await testConsumer.subscribe({ topics: [TOPIC] });

    let messageReceived = false;
    const messageReceivedPromise = new Promise<{
      message: any;
    }>((resolve, reject) => {
      const timeout = setTimeout(() => {
        testConsumer.disconnect().catch(() => {});
        reject(new Error("Timeout waiting for message"));
      }, 15000);

      // Start consumer.run() - it runs continuously
      testConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          if (messageReceived) return;
          const messageKey = message.key?.toString();
          if (topic === TOPIC && messageKey === testKey) {
            messageReceived = true;
            clearTimeout(timeout);
            // Wait a bit for span to be ended before resolving
            await new Promise((resolve) => setTimeout(resolve, 100));
            resolve({
              message: {
                topic,
                partition,
                offset: message.offset,
                key: message.key,
                value: message.value,
                headers: message.headers,
              },
            });
          }
        },
      }).catch((err: any) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    // Wait a bit for consumer to be ready
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await producer.send({
      topic: TOPIC,
      messages: [
        {
          key: testKey,
          value: testValue,
          headers: {
            [customHeaderKey]: customHeaderValue,
          },
        },
      ],
    });

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const received = await messageReceivedPromise;
    
    // Wait a bit for span to be ended
    await new Promise((resolve) => setTimeout(resolve, 500));
    
    // Disconnect consumer to stop the continuous run() loop
    try {
      await testConsumer.disconnect();
    } catch (e) {
      // ignore
    }

    // Check that custom header is preserved
    assert.ok(received.message.headers, "Message should have headers");
    
    // Headers can be an array or object in Promise API
    let headersObj: Record<string, any> = {};
    if (Array.isArray(received.message.headers)) {
      received.message.headers.forEach((h: any) => {
        Object.entries(h).forEach(([key, value]) => {
          headersObj[key] = value;
        });
      });
    } else {
      headersObj = received.message.headers as Record<string, any>;
    }
    
    assert.ok(
      headersObj[customHeaderKey],
      `Custom header '${customHeaderKey}' should be present`
    );
    
    const headerValue = headersObj[customHeaderKey];
    const headerValueStr = Array.isArray(headerValue)
      ? headerValue[0]?.toString("utf8")
      : headerValue?.toString("utf8");
    
    assert.strictEqual(
      headerValueStr,
      customHeaderValue,
      `Custom header value should match. Expected: ${customHeaderValue}, Got: ${headerValueStr}`
    );

    // Check that traceparent header is also present (OTel propagation)
    assert.ok(
      headersObj.traceparent,
      "traceparent header should be present for context propagation"
    );
    const traceparentValue = Array.isArray(headersObj.traceparent)
      ? headersObj.traceparent[0]?.toString("utf8")
      : headersObj.traceparent?.toString("utf8");
    assert.ok(traceparentValue, "traceparent header should have a value");
  });
});

