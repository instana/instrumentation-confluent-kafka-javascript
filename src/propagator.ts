import { TextMapGetter, defaultTextMapGetter } from "@opentelemetry/api";

export const bufferTextMapGetter: TextMapGetter = {
  get(carrier, key) {
    if (!carrier) {
      return undefined;
    }

    const keys = Object.keys(carrier);

    for (const carrierKey of keys) {
      if (carrierKey === key || carrierKey.toLowerCase() === key.toLowerCase()) {
        const value = carrier[carrierKey];
        if (Buffer.isBuffer(value)) {
          return value.toString("utf8");
        }
        if (typeof value === "string") {
          return value;
        }
        if (value !== undefined && value !== null) {
          return String(value);
        }
        return undefined;
      }
    }

    return undefined;
  },

  keys(carrier) {
    if (!carrier) {
      return [];
    }
    return Object.keys(carrier);
  },
};

export const stringTextMapGetter: TextMapGetter = {
  get(carrier, key) {
    if (!carrier) {
      return undefined;
    }
    return defaultTextMapGetter.get(carrier, key);
  },
  keys(carrier) {
    if (!carrier) {
      return [];
    }
    return defaultTextMapGetter.keys(carrier);
  },
};
