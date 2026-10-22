
import type { ConsumeMessage } from "amqplib";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const DATA_DIR = "/data";
const PROCESSED_IDS_FILE = `${DATA_DIR}/processed-ids.json`;
const NOTIFICATION_LOG_FILE = `${DATA_DIR}/notification.log`;

async function readProcessedIds(): Promise<Set<string>> {
  try {
    const content = await readFile(PROCESSED_IDS_FILE, "utf8");
    const ids: unknown = JSON.parse(content);
    if (!Array.isArray(ids)) {
      throw new Error("Processed IDs file must contain a JSON array");
    }
    return new Set(ids.filter((id): id is string => typeof id === "string"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return new Set<string>();
    }
    throw error;
  }
}

async function main(): Promise<void> {
  await mkdir(DATA_DIR, { recursive: true });

  // Ensure the persistent file exists before consuming messages.
  try {
    await readFile(PROCESSED_IDS_FILE, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    await writeFile(PROCESSED_IDS_FILE, "[]\n", "utf8");
  }

  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    let retryCount = getRetryCount(msg);

    try {
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      const correlationId = msg.properties.headers?.["correlationId"] as
        | string
        | undefined;

      if (!correlationId) {
        throw new Error("Missing correlationId message header");
      }

      const processedIds = await readProcessedIds();

      if (processedIds.has(correlationId)) {
        channel.ack(msg);
        console.log(`[Notification] duplicate skipped: ${correlationId}`);
        return;
      }

      const notification = {
        correlationId,
        orderId: order.orderId,
        customerEmail: order.customer.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };

      await appendFile(
        NOTIFICATION_LOG_FILE,
        `${JSON.stringify(notification)}\n`,
        "utf8",
      );

      processedIds.add(correlationId);
      await writeFile(
        PROCESSED_IDS_FILE,
        `${JSON.stringify([...processedIds])}\n`,
        "utf8",
      );

      const result = {
        correlationId,
        source: "notification",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Notification recorded" },
      };

      channel.ack(msg);
      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(JSON.stringify(result)),
        {
          headers: { correlationId },
          contentType: "application/json",
          persistent: true,
        },
      );

      console.log(`[Notification] Recorded for order ${order.orderId}`);
    } catch (err) {
      retryCount = getRetryCount(msg);

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE!, "", msg.content, {
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(
          `[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`,
        );
      }
    }
  });
}

main().catch(console.error);
