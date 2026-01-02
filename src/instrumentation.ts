import {
  InstrumentationBase,
  InstrumentationModuleDefinition,
  InstrumentationNodeModuleDefinition,
  isWrapped,
} from "@opentelemetry/instrumentation";
import {
  ConfluentKafkaInstrumentationConfig,
  EventListener,
  KafkaConsumerEvents,
  KafkaProducerEvents,
} from "./types";
import type {
  KafkaConsumer,
  Producer,
  Message,
  NumberNullUndefined,
  MessageValue,
  MessageKey,
  MessageHeader,
} from "@confluentinc/kafka-javascript/types/rdkafka";
import type {
  Kafka as KafkaJSKafka,
  Producer as KafkaJSProducer,
  Consumer as KafkaJSConsumer,
  ProducerRecord,
  EachMessageHandler,
  EachMessagePayload,
  IHeaders,
} from "@confluentinc/kafka-javascript/types/kafkajs";

type ConfluentKafkaModule = {
  Producer: typeof Producer;
  KafkaConsumer: typeof KafkaConsumer;
  KafkaJS?: {
    Kafka: typeof KafkaJSKafka;
  };
};
import {
  context,
  diag,
  propagation,
  ROOT_CONTEXT,
  SpanKind,
  SpanStatusCode,
  trace,
  defaultTextMapGetter,
} from "@opentelemetry/api";
import { isPromise } from "node:util/types";
import {
  ATTR_MESSAGING_CLIENT_ID,
  ATTR_MESSAGING_CONSUMER_GROUP_NAME,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_KAFKA_OFFSET,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_SYSTEM,
  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA,
} from "@opentelemetry/semantic-conventions/incubating";

// @ts-ignore
import { version, name } from "../package.json";

const KAFKAJS_PROMISE_API_CONTEXT_KEY = Symbol.for("kafkajs-promise-api");

export class ConfluentKafkaInstrumentation extends InstrumentationBase {
  protected declare _config: ConfluentKafkaInstrumentationConfig;

  constructor(config: ConfluentKafkaInstrumentationConfig = {}) {
    super(name, version, config);
  }

  protected init():
    | void
    | InstrumentationModuleDefinition
    | InstrumentationModuleDefinition[] {
    const module = new InstrumentationNodeModuleDefinition(
      "@confluentinc/kafka-javascript",
      [">=1.3.0"],
      this.patch.bind(this),
      this.unpatch.bind(this)
    );
    return module;
  }

  protected patch(moduleExports: ConfluentKafkaModule): ConfluentKafkaModule {
    diag.debug("confluent-kafka instrumentation: applying patch");
    moduleExports = (moduleExports as any)?.default ?? moduleExports;
    this.unpatch(moduleExports);
    this._wrap(
      moduleExports?.Producer.prototype,
      "produce",
      this._patchProduce()
    );
    this._wrap(
      moduleExports?.Producer.prototype,
      "on",
      this._patchProducerOn()
    );
    this._wrap(moduleExports?.KafkaConsumer.prototype, "on", this._patchOn());
    
    if (moduleExports?.KafkaJS?.Kafka) {
      this._wrap(
        moduleExports.KafkaJS.Kafka.prototype,
        "producer",
        this._patchKafkaProducer()
      );
      this._wrap(
        moduleExports.KafkaJS.Kafka.prototype,
        "consumer",
        this._patchKafkaConsumer()
      );
    }
    
    return moduleExports;
  }

  protected unpatch(moduleExports: ConfluentKafkaModule): void {
    diag.debug("confluent-kafka instrumentation: un-patching");
    moduleExports = (moduleExports as any)?.default ?? moduleExports;
    if (isWrapped(moduleExports?.Producer?.prototype.produce)) {
      this._unwrap(moduleExports.Producer.prototype, "produce");
    }
    if (isWrapped(moduleExports?.Producer?.prototype.on)) {
      this._unwrap(moduleExports.Producer.prototype, "on");
    }
    if (isWrapped(moduleExports?.KafkaConsumer?.prototype.on)) {
      this._unwrap(moduleExports.KafkaConsumer.prototype, "on");
    }
    
    if (moduleExports?.KafkaJS?.Kafka) {
      if (isWrapped(moduleExports.KafkaJS.Kafka.prototype.producer)) {
        this._unwrap(moduleExports.KafkaJS.Kafka.prototype, "producer");
      }
      if (isWrapped(moduleExports.KafkaJS.Kafka.prototype.consumer)) {
        this._unwrap(moduleExports.KafkaJS.Kafka.prototype, "consumer");
      }
    }
  }

  private _patchOn() {
    const self = this;
    return (original: KafkaConsumer["on"]) => {
      return function (
        this: any,
        ev: KafkaConsumerEvents,
        originalListener: EventListener<KafkaConsumerEvents>
      ) {
        const eventName = ev;
        if (eventName !== "data") {
          return original.apply(this, [ev, originalListener]);
        }
        const wrappedListener = function (
          this: any,
          data: Message
        ) {
          const headerCarrier: any = {};
          if (data?.headers) {
            data.headers.forEach((header) => {
              Object.entries(header).forEach(([key, value]) => {
                if (value !== undefined && value !== null) {
                  headerCarrier[key] = Buffer.isBuffer(value) 
                    ? value.toString("utf8") 
                    : typeof value === "string" 
                      ? value 
                      : String(value);
                }
              });
            });
          }
          const ctx = Object.keys(headerCarrier).length > 0
            ? propagation.extract(ROOT_CONTEXT, headerCarrier, defaultTextMapGetter)
            : ROOT_CONTEXT;
          const span = self.tracer.startSpan(
            data.topic ?? eventName,
            {
              kind: SpanKind.CONSUMER,
              attributes: {
                [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                [ATTR_MESSAGING_DESTINATION_NAME]: data.topic,
                [ATTR_MESSAGING_OPERATION_NAME]: "received",
                [ATTR_MESSAGING_OPERATION_TYPE]:
                  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
                [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: data.key?.toString(),
                [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
                  data?.key && data.value === null ? true : undefined,
                [ATTR_MESSAGING_KAFKA_OFFSET]: data.offset.toString(),
                [ATTR_MESSAGING_CONSUMER_GROUP_NAME]: data.headers
                  ?.find((h) => Object.keys(h).some(key => key === "group-id"))
                  ?.["group-id"]?.toString(),
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]:
                  data.partition.toString(),
                [ATTR_MESSAGING_CLIENT_ID]: data.headers
                  ?.find((h) => Object.keys(h).some(key => key === "client-id"))
                  ?.["client-id"]?.toString(),
              },
            },
            ctx
          );
          if (self._config.consumerHook) {
            self._config.consumerHook(span, {
              topic: data?.topic,
              message: data,
            });
          }
          return context.with(trace.setSpan(context.active(), span), () =>
            self.endSpan(
              () =>
                (originalListener as EventListener<"data">).apply(this, [data]),
              span
            )
          );
        };
        return original.apply(this, [ev, wrappedListener]);
      };
    };
  }

  private _patchProducerOn() {
    return (original: Producer["on"]) => {
      return function (
        this: any,
        ev: KafkaProducerEvents,
        originalListener: EventListener<KafkaProducerEvents>
      ) {
        return original.apply(this, [ev, originalListener]);
      };
    };
  }

  private _patchProduce() {
    const self = this;
    return (original: Producer["produce"]) => {
      return function (
        this: any,
        topic: string,
        partition: NumberNullUndefined,
        message: MessageValue,
        key?: MessageKey,
        timestamp?: NumberNullUndefined,
        opaque?: any,
        headers?: MessageHeader[]
      ) {
        const activeContext = context.active();
        const isPromiseApiCall = activeContext.getValue(KAFKAJS_PROMISE_API_CONTEXT_KEY) === true;
        
        if (isPromiseApiCall) {
          return original.apply(this, [
            topic,
            partition,
            message,
            key,
            timestamp,
            opaque,
            headers,
          ]);
        }
        
        const span = self.tracer.startSpan(topic, {
          kind: SpanKind.PRODUCER,
          attributes: {
            [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
            [ATTR_MESSAGING_DESTINATION_NAME]: topic,
            [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: key?.toString(),
            [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
              key && message === null ? true : undefined,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition?.toString(),
            [ATTR_MESSAGING_OPERATION_NAME]: "produce",
            [ATTR_MESSAGING_OPERATION_TYPE]:
              MESSAGING_OPERATION_TYPE_VALUE_SEND,
            [ATTR_MESSAGING_CLIENT_ID]: (headers ?? [])
              .find((h) => Object.keys(h).some(key => key === "client-id"))
              ?.["client-id"]?.toString(),
          },
        });
        const ctx = trace.setSpan(context.active(), span);
        
        const carrier: any = {};
        propagation.inject(ctx, carrier);
        
        const kafkaHeaders: MessageHeader[] = headers ? headers.map(h => ({ ...h })) : [];
        
        Object.entries(carrier).forEach(([k, v]) => {
          if (!v) return;
          const headerValue = typeof v === "string" ? v : String(v);
          const existingIndex = kafkaHeaders.findIndex((h) => {
            return Object.keys(h).some(key => key === k);
          });
          if (existingIndex >= 0) {
            kafkaHeaders[existingIndex] = { ...kafkaHeaders[existingIndex], [k]: Buffer.from(headerValue, "utf8") };
          } else {
            kafkaHeaders.push({ [k]: Buffer.from(headerValue, "utf8") });
          }
        });
        
        propagation.inject(ctx, opaque ?? {});
        if (self._config.producerHook) {
          self._config.producerHook(span, {
            topic,
            message: {
              topic,
              partition: partition ?? 0,
              offset: -1,
              size: 0,
              value: message,
              key: key,
              timestamp: timestamp ?? 0,
              opaque: opaque,
              headers: kafkaHeaders,
            },
          });
        }
        return context.with(ctx, () =>
          self.endSpan(
            () =>
              original.apply(this, [
                topic,
                partition,
                message,
                key,
                timestamp,
                opaque,
                kafkaHeaders,
              ]),
            span
          )
        );
      };
    };
  }

  private _patchKafkaProducer() {
    const self = this;
    return (original: any) => {
      return function (this: any, config: any) {
        const producer = original.apply(this, [config]);
        if (producer && typeof producer.send === "function") {
          self._wrap(producer, "send", self._patchProducerSend());
        }
        return producer;
      };
    };
  }

  private _patchKafkaConsumer() {
    const self = this;
    return (original: any) => {
      return function (this: any, config: any) {
        const consumer = original.apply(this, [config]);
        if (consumer && typeof consumer.run === "function") {
          self._wrap(consumer, "run", self._patchConsumerRun());
        }
        return consumer;
      };
    };
  }

  private _patchProducerSend() {
    const self = this;
    return (original: KafkaJSProducer["send"]) => {
      return async function (this: any, record: ProducerRecord) {
        const topic = record.topic;
        const messages = record.messages || [];
        
        const spans: any[] = [];
        const ctxs: any[] = [];
        const modifiedMessages: any[] = [];
        
        const promiseApiContext = context.active().setValue(KAFKAJS_PROMISE_API_CONTEXT_KEY, true);
        
        for (const message of messages) {
          
          const span = self.tracer.startSpan(topic, {
            kind: SpanKind.PRODUCER,
            attributes: {
              [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
              [ATTR_MESSAGING_DESTINATION_NAME]: topic,
              [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: message.key?.toString(),
              [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
                message.key && !message.value ? true : undefined,
              [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: message.partition?.toString(),
              [ATTR_MESSAGING_OPERATION_NAME]: "send",
              [ATTR_MESSAGING_OPERATION_TYPE]:
                MESSAGING_OPERATION_TYPE_VALUE_SEND,
            },
          });
          
          const ctx = trace.setSpan(promiseApiContext, span);
          ctxs.push(ctx);
          spans.push(span);
          
          const carrier: any = {};
          propagation.inject(ctx, carrier);
          
          const headers: IHeaders = message.headers ? { ...message.headers } : {};
          Object.entries(carrier).forEach(([k, v]) => {
            if (!v) return;
            const headerValue = typeof v === "string" ? v : String(v);
            const headerBuffer = Buffer.from(headerValue, "utf8");
            if (headers[k] !== undefined) {
              const existing = headers[k];
              if (Array.isArray(existing)) {
                headers[k] = [...existing, headerBuffer];
              } else if (existing !== undefined) {
                headers[k] = [existing, headerBuffer];
              } else {
                headers[k] = headerBuffer;
              }
            } else {
              headers[k] = headerBuffer;
            }
          });
          
          const modifiedMessage = {
            ...message,
            headers: headers,
          };
          modifiedMessages.push(modifiedMessage);
          
          if (self._config.producerHook) {
            const convertedHeaders: MessageHeader[] = Object.entries(headers).map(([k, v]) => {
              if (Array.isArray(v)) {
                return { [k]: v[0] } as MessageHeader;
              } else if (typeof v === "string") {
                return { [k]: Buffer.from(v, "utf8") } as MessageHeader;
              } else {
                return { [k]: v } as MessageHeader;
              }
            });
            
            const messageValue: MessageValue = message.value
              ? typeof message.value === "string"
                ? Buffer.from(message.value, "utf8")
                : message.value
              : null;
            
            const messageKey: MessageKey = message.key
              ? typeof message.key === "string"
                ? Buffer.from(message.key, "utf8")
                : message.key
              : null;
            
            self._config.producerHook(span, {
              topic,
              message: {
                topic,
                partition: message.partition ?? 0,
                offset: -1,
                size: 0,
                value: messageValue,
                key: messageKey,
                timestamp: message.timestamp ? (typeof message.timestamp === "string" ? parseInt(message.timestamp) : message.timestamp) : 0,
                headers: convertedHeaders,
              },
            });
          }
        }
        
        const modifiedRecord: ProducerRecord = {
          topic,
          messages: modifiedMessages,
        };
        
        try {
          const result = await context.with(promiseApiContext, async () => {
            return await original.apply(this, [modifiedRecord]);
          });
          
          spans.forEach((span) => span.end());
          
          return result;
        } catch (error) {
          spans.forEach((span) => {
            if (error instanceof Error) {
              span.recordException(error);
            }
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: error instanceof Error ? error.message : String(error),
            });
            span.end();
          });
          throw error;
        }
      };
    };
  }

  private _patchConsumerRun() {
    const self = this;
    return (original: KafkaJSConsumer["run"]) => {
      return async function (this: any, config?: { eachMessage?: EachMessageHandler }) {
        const originalEachMessage = config?.eachMessage;
        
        const wrappedEachMessage: EachMessageHandler = async (payload: EachMessagePayload) => {
          const params = {
            topic: payload.topic,
            partition: payload.partition,
            message: payload.message,
          };
          const headerCarrier: any = {};
          if (params.message.headers) {
            const headers = params.message.headers;
            
            Object.entries(headers).forEach(([key, value]) => {
              if (value !== undefined && value !== null) {
                const actualValue = Array.isArray(value) ? value[0] : value;
                headerCarrier[key] = Buffer.isBuffer(actualValue) 
                  ? actualValue.toString("utf8") 
                  : typeof actualValue === "string" 
                    ? actualValue 
                    : String(actualValue);
              }
            });
          }
          
          const ctx = Object.keys(headerCarrier).length > 0
            ? propagation.extract(ROOT_CONTEXT, headerCarrier, defaultTextMapGetter)
            : ROOT_CONTEXT;
          
          const span = self.tracer.startSpan(
            payload.topic,
            {
              kind: SpanKind.CONSUMER,
              attributes: {
                [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                [ATTR_MESSAGING_DESTINATION_NAME]: payload.topic,
                [ATTR_MESSAGING_OPERATION_NAME]: "received",
                [ATTR_MESSAGING_OPERATION_TYPE]:
                  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
                [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: payload.message.key?.toString() ?? undefined,
                [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
                  payload.message.key && !payload.message.value ? true : undefined,
                [ATTR_MESSAGING_KAFKA_OFFSET]: payload.message.offset,
                [ATTR_MESSAGING_DESTINATION_PARTITION_ID]:
                  payload.partition.toString(),
              },
            },
            ctx
          );
          if (self._config.consumerHook) {
            let convertedHeaders: MessageHeader[] | undefined = undefined;
            if (payload.message.headers) {
              const headers = payload.message.headers;
              convertedHeaders = Object.entries(headers).map(([key, value]) => {
                if (Array.isArray(value)) {
                  return { [key]: Buffer.isBuffer(value[0]) ? value[0] : Buffer.from(value[0] as string, "utf8") } as MessageHeader;
                } else if (typeof value === "string") {
                  return { [key]: Buffer.from(value, "utf8") } as MessageHeader;
                } else {
                  return { [key]: value } as MessageHeader;
                }
              });
            }
            
            self._config.consumerHook(span, {
              topic: payload.topic,
              message: {
                topic: payload.topic,
                partition: payload.partition,
                offset: parseInt(payload.message.offset),
                size: payload.message.value ? payload.message.value.length : 0,
                value: payload.message.value ?? null,
                key: payload.message.key ?? null,
                headers: convertedHeaders,
              },
            });
          }
          
          return context.with(trace.setSpan(ctx, span), async () => {
            try {
              if (originalEachMessage) {
                await originalEachMessage(payload);
              }
              span.end();
            } catch (error) {
              if (error instanceof Error) {
                span.recordException(error);
              }
              span.setStatus({
                code: SpanStatusCode.ERROR,
                message: error instanceof Error ? error.message : String(error),
              });
              span.end();
              throw error;
            }
          });
        };
        
        return original.apply(this, [{ ...config, eachMessage: wrappedEachMessage }]);
      };
    };
  }

  private endSpan(traced: any, span: any) {
    try {
      const result = traced();
      if (isPromise(result)) {
        return result.then(
          (value) => {
            span.end();
            return value;
          },
          (err) => {
            span.recordException(err);
            span.setStatus({
              code: SpanStatusCode.ERROR,
              message: err?.message,
            });
            span.end();
            throw err;
          }
        );
      } else {
        span.end();
        return result;
      }
    } catch (error) {
      span.recordException(error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: error instanceof Error ? error.message : String(error),
      });
      span.end();
      throw error;
    }
  }
}
