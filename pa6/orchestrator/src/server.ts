
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

interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

type SagaState = "completed" | "failed" | "compensated";
type IdempotencyState =
  | "in_progress"
  | "completed"
  | "failed"
  | "compensated";

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

interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  unitPrice: string;
  currency: string;
  taxRate: number;
}

interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  receivedAt: string;
  orderDate: string;
  customer: {
    name: string;
    email: string;
    address: {
      street: string;
      city: string;
      postalCode: string;
      country: string;
    };
  };
  items: CanonicalItem[];
  currency: string;
  status: "new" | "processing" | "shipped" | "delivered";
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);

  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }

  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(
      filePath,
      JSON.stringify(initialData, null, 2),
      "utf8"
    );
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  const temporaryPath = `${filePath}.tmp`;

  fs.writeFileSync(
    temporaryPath,
    JSON.stringify(value, null, 2),
    "utf8"
  );

  fs.renameSync(temporaryPath, filePath);
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto
    .createHash("sha256")
    .update(normalized)
    .digest("hex");

  return `sha256:${hash}`;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value)
  );
}

function hasExactFields(
  value: Record<string, unknown>,
  allowed: string[]
): boolean {
  return Object.keys(value).every((key) => allowed.includes(key));
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isDateTime(value: unknown): boolean {
  return (
    typeof value === "string" &&
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(
      value
    ) &&
    Number.isFinite(Date.parse(value))
  );
}

function validateCheckoutPayload(payload: unknown): string | null {
  if (!isObject(payload)) {
    return "Request body must be a JSON object";
  }

  const orderFields = [
    "orderId",
    "orderType",
    "source",
    "receivedAt",
    "orderDate",
    "customer",
    "items",
    "currency",
    "status",
  ];

  if (!hasExactFields(payload, orderFields)) {
    return "Order contains unexpected fields";
  }

  for (const field of orderFields) {
    if (payload[field] === undefined || payload[field] === null) {
      return `Field "${field}" is required`;
    }
  }

  if (!isNonEmptyString(payload.orderId)) {
    return "orderId must be a non-empty string";
  }

  if (
    !["standard", "express", "b2b"].includes(
      payload.orderType as string
    )
  ) {
    return "Invalid orderType";
  }

  if (!["web", "mobile", "b2b"].includes(payload.source as string)) {
    return "Invalid source";
  }

  if (!isDateTime(payload.receivedAt) || !isDateTime(payload.orderDate)) {
    return "Invalid order date-time";
  }

  if (
    !["new", "processing", "shipped", "delivered"].includes(
      payload.status as string
    )
  ) {
    return "Invalid order status";
  }

  if (
    typeof payload.currency !== "string"
  ) {
    return "Invalid order currency";
  }

  if (!isObject(payload.customer)) {
    return "Invalid customer";
  }

  const customer = payload.customer;

  if (
    !hasExactFields(customer, ["name", "email", "address"]) ||
    !isNonEmptyString(customer.name) ||
    typeof customer.email !== "string" ||
    !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(customer.email)
  ) {
    return "Invalid customer name or email";
  }

  if (!isObject(customer.address)) {
    return "Invalid customer address";
  }

  const address = customer.address;

  if (
    !hasExactFields(address, [
      "street",
      "city",
      "postalCode",
      "country",
    ]) ||
    !isNonEmptyString(address.street) ||
    !isNonEmptyString(address.city) ||
    !isNonEmptyString(address.postalCode) ||
    typeof address.country !== "string"
  ) {
    return "Invalid customer address";
  }

  if (
    !Array.isArray(payload.items) ||
    payload.items.length < 1 ||
    payload.items.length > 100
  ) {
    return "Order must contain between 1 and 100 items";
  }

  for (const item of payload.items) {
    if (!isObject(item)) {
      return "Invalid order item";
    }

    if (
      !hasExactFields(item, [
        "productId",
        "productName",
        "quantity",
        "unitPrice",
        "currency",
        "taxRate",
      ]) ||
      typeof item.productId !== "string" ||
      !/^PROD-[0-9]+$/.test(item.productId) ||
      !isNonEmptyString(item.productName) ||
      !Number.isInteger(item.quantity) ||
      (item.quantity as number) < 1 ||
      typeof item.unitPrice !== "string" ||
      !/^-?[0-9]+\.[0-9]{2}$/.test(item.unitPrice) ||
      typeof item.currency !== "string" ||
      typeof item.taxRate !== "number" ||
      !Number.isFinite(item.taxRate) ||
      item.taxRate < 0 ||
      item.taxRate > 1
    ) {
      return "Invalid order item";
    }
  }

  return null;
}

function calculateOrderAmount(items: CanonicalItem[]): string {
  let totalCents = 0n;

  for (const item of items) {
    const negative = item.unitPrice.startsWith("-");
    const unsigned = negative
      ? item.unitPrice.slice(1)
      : item.unitPrice;

    const [whole, fraction] = unsigned.split(".");

    const cents =
     BigInt(whole ?? "0") * 100n + BigInt(fraction ?? "00");

    totalCents +=
      (negative ? -cents : cents) * BigInt(item.quantity);
  }

  const negative = totalCents < 0n;
  const absolute = negative ? -totalCents : totalCents;

  const whole = absolute / 100n;
  const fraction = (absolute % 100n).toString().padStart(2, "0");

  return `${negative ? "-" : ""}${whole}.${fraction}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

async function callService(
  url: string,
  body: unknown,
  orderId: string
): Promise<{ status: number; data: any }> {
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
  startedAt: number
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
  trace: TraceItem[]
): void {
  const store = readJsonFile<SagaStore>(SAGA_STORE_PATH);

  if (!store.sagas) {
    store.sagas = {};
  }

  store.sagas[orderId] = {
    idempotencyKey,
    state,
    steps: [...trace],
    updatedAt: nowIso(),
  };

  writeJsonFile(SAGA_STORE_PATH, store);
}

function saveIdempotency(
  idempotencyKey: string,
  requestHash: string,
  state: IdempotencyState,
  httpStatus: number,
  response: unknown
): void {
  const store = readJsonFile<IdempotencyStore>(
    IDEMPOTENCY_STORE_PATH
  );

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
  const orderId = req.params.orderId ?? "";
  const store = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = store.sagas?.[orderId];

  if (!saga) {
    res.status(404).json({
      code: "not_found",
      message: "No saga found for this orderId",
    });
    return;
  }

  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key") ?? "";

  if (!idempotencyKey.trim()) {
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

  const order = req.body as CanonicalOrder;
  const orderId = order.orderId;
  const requestHash = payloadHash(order);

  const store = readJsonFile<IdempotencyStore>(
    IDEMPOTENCY_STORE_PATH
  );

  if (!store.records) {
    store.records = {};
  }

  const existing = store.records[idempotencyKey];

  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This key was used with a different payload",
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

  saveIdempotency(
    idempotencyKey,
    requestHash,
    "in_progress",
    202,
    {
      orderId,
      status: "in_progress",
      trace: [],
    }
  );

  const trace: TraceItem[] = [];

  let paymentCompleted = false;
  let inventoryCompleted = false;
  let completed = false;

  let timedOut = false;
  let compensationFailed = false;
  let failureCode = "checkout_failed";

  async function runStep(
    step: string,
    url: string,
    body: unknown,
    isCompensation = false
  ): Promise<boolean> {
    const startedAt = Date.now();

    try {
      const result = await callService(url, body, orderId);
      const success =
        result.status >= 200 && result.status < 300;

      trace.push(
        createTraceItem(
          step,
          success ? "success" : "failed",
          startedAt
        )
      );

      if (!success && !isCompensation) {
        failureCode =
          typeof result.data?.code === "string"
            ? result.data.code
            : "checkout_failed";
      }

      saveSaga(orderId, idempotencyKey, "failed", trace);

      return success;
    } catch (error) {
      const isTimeout =
        axios.isAxiosError(error) &&
        (error.code === "ECONNABORTED" ||
          error.code === "ETIMEDOUT");

      trace.push(
        createTraceItem(
          step,
          isTimeout ? "timeout" : "failed",
          startedAt
        )
      );

      if (!isCompensation) {
        timedOut = isTimeout;
        failureCode = isTimeout ? "timeout" : "checkout_failed";
      }

      saveSaga(orderId, idempotencyKey, "failed", trace);

      return false;
    }
  }

  async function compensate(
    step: string,
    url: string
  ): Promise<void> {
    const success = await runStep(
      step,
      url,
      { orderId },
      true
    );

    if (!success) {
      compensationFailed = true;
    }
  }

  try {
    const paymentOk = await runStep(
      "payment",
      `${config.paymentUrl}/payment/authorize`,
      {
        orderId,
        amount: calculateOrderAmount(order.items),
      }
    );

    if (paymentOk) {
      paymentCompleted = true;

      const inventoryOk = await runStep(
        "inventory",
        `${config.inventoryUrl}/inventory/reserve`,
        {
          orderId,
          items: order.items,
        }
      );

      if (inventoryOk) {
        inventoryCompleted = true;

        const shippingOk = await runStep(
          "shipping",
          `${config.shippingUrl}/shipping/create`,
          { orderId }
        );

        if (shippingOk) {
          const notificationOk = await runStep(
            "notification",
            `${config.notificationUrl}/notification/send`,
            {
              orderId,
              recipient: order.customer.email,
            }
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
        `${config.inventoryUrl}/inventory/release`
      );
    }

    if (paymentCompleted) {
      await compensate(
        "payment_refund",
        `${config.paymentUrl}/payment/refund`
      );
    }
  }

  const finalStatus: SagaState = completed
    ? "completed"
    : compensationFailed
      ? "failed"
      : paymentCompleted || inventoryCompleted
        ? "compensated"
        : "failed";

  const httpStatus = completed
    ? 200
    : compensationFailed
      ? 422
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

    responseBody.message = compensationFailed
      ? "One or more compensation steps failed"
      : "Checkout could not be completed";
  }

  saveSaga(orderId, idempotencyKey, finalStatus, trace);

  saveIdempotency(
    idempotencyKey,
    requestHash,
    finalStatus,
    httpStatus,
    responseBody
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
