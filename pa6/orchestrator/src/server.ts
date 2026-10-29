/**
 * PA6 orchestrator scaffold — ported from the practice-03-orchestration JS lab.
 *
 * This scaffold is intentionally incomplete. `GET /health`, request validation,
 * the Idempotency-Key contract shell, and restart-safe file persistence are
 * already wired up. What is NOT implemented is the actual saga:
 *
 *   1) payment authorize
 *   2) inventory reserve
 *   3) shipping create
 *   4) notification send
 *
 * with strict sequencing, trace recording, timeout handling, compensation
 * (in reverse order of completed steps), and the full idempotent-replay
 * policy. See the TODO block inside the POST /checkout handler.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import axios from "axios";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

function validateCheckoutPayload(payload: any): string | null {
  if (!payload || typeof payload !== "object") {
    return "Request body must be a JSON object";
  }
  if (typeof payload.orderId !== "string" || payload.orderId.trim() === "") {
    return 'Field "orderId" is required and must be a non-empty string';
  }
  if (!Array.isArray(payload.items) || payload.items.length === 0) {
    return 'Field "items" is required and must be a non-empty array';
  }
  if (typeof payload.amount !== "number") {
    return 'Field "amount" is required and must be numeric';
  }
  if (typeof payload.recipient !== "string" || payload.recipient.trim() === "") {
    return 'Field "recipient" is required and must be a non-empty string';
  }
  return null;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}
async function callService(
  url: string,
  body: unknown,
  orderId: string,
): Promise<any> {
  const response = await axios.post(url, body, {
    timeout: config.requestTimeoutMs,
    headers: {
      "x-order-id": orderId,
      "x-correlation-id": orderId,
    },
    validateStatus: () => true,
  });

  return {
    status: response.status,
    data: response.data,
  };
}
function createTraceItem(
  step: string,
  status: string,
  startedAt: number,
): TraceItem {
  const finishedAt = Date.now();

  return {
    step,
    status,
    startedAt: new Date(startedAt).toISOString(),
    finishedAt: new Date(finishedAt).toISOString(),
    durationMs: finishedAt - startedAt,
  };
}
function saveSaga(
  orderId: string,
  idempotencyKey: string,
  state: SagaState,
  trace: TraceItem[],
): void {
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);

  if (!sagaStore.sagas) {
    sagaStore.sagas = {};
  }

  sagaStore.sagas[orderId] = {
    idempotencyKey,
    state,
    steps: trace,
    updatedAt: nowIso(),
  };

  writeJsonFile(SAGA_STORE_PATH, sagaStore);
}

function saveIdempotency(
  idempotencyKey: string,
  requestHash: string,
  state: IdempotencyState,
  httpStatus: number,
  response: unknown,
): void {
  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);

  if (!store.records) {
    store.records = {};
  }

  store.records[idempotencyKey] = {
    requestHash,
    state,
    httpStatus,
    response,
    updatedAt: nowIso(),
  };

  writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key") ?? "";

  if (!idempotencyKey) {
    res.status(400).json({
      code: "validation_error",
      message: "Idempotency-Key header is required",
    });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({
      code: "validation_error",
      message: validationError,
    });
    return;
  }

  const orderId: string = req.body.orderId;
  const requestHash = payloadHash(req.body);
  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);

  if (!store.records) store.records = {};

  const existing = store.records[idempotencyKey];

  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }

    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this key is still in progress",
      });
      return;
    }

    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  saveIdempotency(idempotencyKey, requestHash, "in_progress", 202, {
    orderId,
    status: "in_progress",
  });

  const trace: TraceItem[] = [];
  let paymentCompleted = false;
  let inventoryCompleted = false;
  let timedOut = false;
  let compensationFailed = false;
  let failureCode = "checkout_failed";

  async function runStep(
    step: string,
    url: string,
    body: unknown,
  ): Promise<boolean> {
    const startedAt = Date.now();

    try {
      const result = await callService(url, body, orderId);
      const success = result.status >= 200 && result.status < 300;

      trace.push(createTraceItem(
        step,
        success ? "success" : "failed",
        startedAt,
      ));

      if (!success) {
        failureCode = typeof result.data?.code === "string"
          ? result.data.code
          : "checkout_failed";
      }

      saveSaga(orderId, idempotencyKey, "failed", trace);
      return success;
    } catch (error) {
      const isTimeout =
        axios.isAxiosError(error) &&
        (error.code === "ECONNABORTED" || error.code === "ETIMEDOUT");

      timedOut = isTimeout;
      failureCode = isTimeout ? "timeout" : "checkout_failed";

      trace.push(createTraceItem(step, "failed", startedAt));
      saveSaga(orderId, idempotencyKey, "failed", trace);
      return false;
    }
  }

  async function compensate(
    step: string,
    url: string,
  ): Promise<void> {
    const success = await runStep(step, url, { orderId });

    if (!success) compensationFailed = true;
  }

  let completed = false;

  try {
    const paymentOk = await runStep(
      "payment",
      `${config.paymentUrl}/payment/authorize`,
      {
        orderId,
        amount: req.body.amount,
      },
    );

    if (paymentOk) {
      paymentCompleted = true;

      const inventoryOk = await runStep(
        "inventory",
        `${config.inventoryUrl}/inventory/reserve`,
        {
          orderId,
          items: req.body.items,
        },
      );

      if (inventoryOk) {
        inventoryCompleted = true;

        const shippingOk = await runStep(
          "shipping",
          `${config.shippingUrl}/shipping/create`,
          { orderId },
        );

        if (shippingOk) {
          const notificationOk = await runStep(
            "notification",
            `${config.notificationUrl}/notification/send`,
            {
              orderId,
              recipient: req.body.recipient,
            },
          );

          completed = notificationOk;
        }
      }
    }
  } catch {
    failureCode = "checkout_failed";
  }

  if (!completed) {
    if (inventoryCompleted) {
      await compensate(
        "inventory_release",
        `${config.inventoryUrl}/inventory/release`,
      );
    }

    if (paymentCompleted) {
      await compensate(
        "payment_refund",
        `${config.paymentUrl}/payment/refund`,
      );
    }
  }

  const finalStatus = completed
    ? "completed"
    : compensationFailed
      ? "compensated"
      : "failed";

  const httpStatus = completed
    ? 200
    : timedOut
      ? 504
      : 422;

  const responseBody: Record<string, unknown> = {
    orderId,
    status: finalStatus,
    trace,
  };

  if (!completed) {
    responseBody.code = compensationFailed
      ? "compensation_failed"
      : failureCode;
    responseBody.message = completed
      ? "Checkout completed"
      : "Checkout could not be completed";
  }

  const sagaState: SagaState = completed
    ? "completed"
    : "failed";

  saveSaga(orderId, idempotencyKey, sagaState, trace);

  const idempotencyState: IdempotencyState = completed
    ? "completed"
    : compensationFailed
      ? "compensated"
      : "failed";

  saveIdempotency(
    idempotencyKey,
    requestHash,
    idempotencyState,
    httpStatus,
    responseBody,
  );

  res.status(httpStatus).json(responseBody);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
