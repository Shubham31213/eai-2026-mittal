
/**
 * PA6 mock payment service.
 * Supports payment authorization, refunds,
 * failure simulation, and request logging.
 */

import express, { type Request, type Response } from "express";

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 4001);

type FailMode = "never" | "always" | "random";

interface LogEntry {
  seq: number;
  at: string;
  action: string;
  orderId: string | null;
  correlationId: string | null;
  outcome: string;
}

interface Config {
  paymentFailMode: FailMode;
  paymentRefundFailMode: FailMode;
}

let logs: LogEntry[] = [];
let sequence = 0;

const config: Config = {
  paymentFailMode:
    (process.env.PAYMENT_FAIL_MODE as FailMode | undefined) ||
    "never",

  paymentRefundFailMode:
    (process.env.PAYMENT_REFUND_FAIL_MODE as FailMode | undefined) ||
    "never",
};

function shouldFail(mode: FailMode): boolean {
  if (mode === "always") {
    return true;
  }

  if (mode === "random") {
    return Math.random() < 0.5;
  }

  return false;
}

function record(
  action: string,
  req: Request,
  body: any,
  outcome: string
): void {
  sequence += 1;

  logs.push({
    seq: sequence,
    at: new Date().toISOString(),
    action,
    orderId: body?.orderId || req.header("x-order-id") || null,
    correlationId: req.header("x-correlation-id") || null,
    outcome,
  });
}

// Health check
app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({
    status: "ok",
  });
});

// Payment authorization
app.post("/payment/authorize", (req: Request, res: Response) => {
  const { orderId, amount } = req.body || {};

  // Validate orderId and two-decimal amount string
  if (
    !orderId ||
    typeof amount !== "string" ||
    !/^-?[0-9]+\.[0-9]{2}$/.test(amount)
  ) {
    record("authorize", req, req.body, "validation_error");

    res.status(400).json({
      code: "validation_error",
      message:
        "orderId and a two-decimal amount string are required",
    });

    return;
  }

  // Simulate payment failure
  if (shouldFail(config.paymentFailMode)) {
    record("authorize", req, req.body, "failed");

    res.status(422).json({
      ok: false,
      step: "payment",
      code: "payment_declined",
      orderId,
    });

    return;
  }

  // Successful authorization
  record("authorize", req, req.body, "success");

  res.status(200).json({
    ok: true,
    step: "payment",
    authorizationId: `pay-${orderId}`,
    orderId,
  });
});

// Payment refund
app.post("/payment/refund", (req: Request, res: Response) => {
  const { orderId } = req.body || {};

  if (!orderId) {
    record("refund", req, req.body, "validation_error");

    res.status(400).json({
      code: "validation_error",
      message: "orderId is required",
    });

    return;
  }

  // Simulate refund failure
  if (shouldFail(config.paymentRefundFailMode)) {
    record("refund", req, req.body, "failed");

    res.status(422).json({
      ok: false,
      step: "payment_refund",
      code: "refund_failed",
      orderId,
    });

    return;
  }

  // Successful refund
  record("refund", req, req.body, "success");

  res.status(200).json({
    ok: true,
    step: "payment_refund",
    refundId: `refund-${orderId}`,
    orderId,
  });
});

// View payment logs
app.get("/admin/logs", (_req: Request, res: Response) => {
  res.status(200).json({
    service: "payment",
    logs,
  });
});

// Configure failure modes
app.post("/admin/config", (req: Request, res: Response) => {
  const incoming = req.body || {};

  if (typeof incoming.paymentFailMode === "string") {
    config.paymentFailMode = incoming.paymentFailMode;
  }

  if (typeof incoming.paymentRefundFailMode === "string") {
    config.paymentRefundFailMode =
      incoming.paymentRefundFailMode;
  }

  res.status(200).json({
    status: "ok",
    config,
  });
});

// Reset payment logs
app.post("/admin/reset", (_req: Request, res: Response) => {
  logs = [];
  sequence = 0;

  res.status(200).json({
    status: "ok",
  });
});

// Start payment service
app.listen(PORT, () => {
  console.log(`[payment] mock listening on ${PORT}`);
});
