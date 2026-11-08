
import axios from "axios";
import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

/**
 * Make one HTTP POST request.
 *
 * 4xx: permanent business failure, do not retry.
 * 5xx/network error: temporary failure, allow Temporal to retry.
 */
async function post(
  url: string,
  body: Record<string, unknown>
): Promise<void> {
  try {
    await axios.post(url, body, {
      validateStatus: () => true,
    }).then((response) => {
      if (response.status >= 200 && response.status < 300) {
        return;
      }

      const data = response.data;
      const code =
        typeof data?.code === "string"
          ? data.code
          : `http_${response.status}`;

      const message =
        typeof data?.message === "string"
          ? data.message
          : `HTTP ${response.status}: ${code}`;

      if (response.status >= 400 && response.status < 500) {
        throw ApplicationFailure.nonRetryable(message, code);
      }

      throw ApplicationFailure.retryable(message, code);
    });
  } catch (error) {
    // Preserve Temporal failure classification.
    if (error instanceof ApplicationFailure) {
      throw error;
    }

    // Network failures are temporary.
    throw error;
  }
}

export async function authorizePayment(
  orderId: string,
  amount: string
): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, {
    orderId,
    amount,
  });
}

export async function reserveInventory(
  orderId: string,
  items: CanonicalItem[]
): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, {
    orderId,
    items,
  });
}

export async function createShipment(
  orderId: string
): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, {
    orderId,
  });
}

export async function sendNotification(
  orderId: string,
  recipient: string
): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, {
    orderId,
    recipient,
  });
}

export async function refundPayment(
  orderId: string
): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, {
    orderId,
  });
}

export async function releaseInventory(
  orderId: string
): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, {
    orderId,
  });
}
