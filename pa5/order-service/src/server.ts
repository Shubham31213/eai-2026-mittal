
import express from "express";
import { randomUUID } from "node:crypto";
import { connectWithRetry } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://guest:guest@rabbitmq:5672";

const app = express();
app.use(express.json());

const orders = new Map<string, CanonicalOrder>();

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

app.post("/orders", async (req, res) => {
  const order = req.body as CanonicalOrder;

  if (
    !order ||
    typeof order !== "object" ||
    typeof order.orderId !== "string" ||
    !Array.isArray(order.items) ||
    !order.customer ||
    typeof order.customer.email !== "string"
  ) {
    res.status(400).json({ error: "Invalid canonical order" });
    return;
  }

  try {
    const correlationId = randomUUID();

    channel.publish(
      "orders.exchange",
      "",
      Buffer.from(JSON.stringify(order)),
      {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      },
    );

    orders.set(correlationId, order);

    res.status(201).json({
      correlationId,
      status: "accepted",
    });
  } catch (error) {
    console.error("[order-service] Failed to publish order:", error);
    res.status(500).json({ error: "Failed to publish order" });
  }
});

app.get("/orders/:correlationId", (req, res) => {
  const { correlationId } = req.params;
  const order = orders.get(correlationId);

  if (!order) {
    res.status(404).json({ error: "Order not found" });
    return;
  }

  res.status(200).json({ correlationId, order });
});

let channel: Awaited<
  ReturnType<typeof connectWithRetry>
>["channel"];

async function start() {
  const rabbit = await connectWithRetry(RABBITMQ_URL);
  channel = rabbit.channel;

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`[order-service] listening on port ${PORT}`);
  });
}

start().catch((error) => {
  console.error("[order-service] Startup failed:", error);
  process.exit(1);
});
