
import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";

import type * as activities from "../activities";
import type {
  CanonicalOrder,
  CheckoutResult,
  TraceItem,
} from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

function calculateAmount(order: CanonicalOrder): string {
  const totalCents = order.items.reduce((sum, item) => {
    const [whole, fraction] = item.unitPrice.split(".");
    const unitCents = Number(whole) * 100 + Number(fraction);
    return sum + unitCents * item.quantity;
  }, 0);

  return `${Math.floor(totalCents / 100)}.${String(
    totalCents % 100
  ).padStart(2, "0")}`;
}

function failureCode(error: unknown): string {
  let current: unknown = error;

  while (current instanceof ActivityFailure) {
    current = current.cause;
  }

  if (current instanceof TimeoutFailure) {
    return "timeout";
  }

  if (current instanceof ApplicationFailure) {
    return current.type || "activity_failed";
  }

  return "activity_failed";
}

export async function checkout(
  order: CanonicalOrder
): Promise<CheckoutResult> {
  const trace: TraceItem[] = [];
  const completed: string[] = [];

  async function recordStep(
    step: string,
    action: () => Promise<void>
  ): Promise<void> {
    const startedAt = new Date();

    try {
      await action();

      const finishedAt = new Date();

      trace.push({
        step,
        status: "success",
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
        durationMs: finishedAt.getTime() - startedAt.getTime(),
      });
    } catch (error) {
      const finishedAt = new Date();
      const code = failureCode(error);

      trace.push({
        step,
        status: code === "timeout" ? "timeout" : "failed",
        startedAt: startedAt.toISOString(),
        finishedAt: finishedAt.toISOString(),
        durationMs: finishedAt.getTime() - startedAt.getTime(),
      });

      throw error;
    }
  }

  const amount = calculateAmount(order);

  let failure: string | undefined;

  try {
    await recordStep("payment", () =>
      act.authorizePayment(order.orderId, amount)
    );
    completed.push("payment");

    await recordStep("inventory", () =>
      act.reserveInventory(order.orderId, order.items)
    );
    completed.push("inventory");

    await recordStep("shipping", () =>
      act.createShipment(order.orderId)
    );
    completed.push("shipping");

    await recordStep("notification", () =>
      act.sendNotification(order.orderId, order.customer.email)
    );
    completed.push("notification");

    return {
      orderId: order.orderId,
      status: "completed",
      trace,
    };
  } catch (error) {
    failure = failureCode(error);
  }

  let compensationFailed = false;

  if (completed.includes("inventory")) {
    try {
      await recordStep("inventory_release", () =>
        act.releaseInventory(order.orderId)
      );
    } catch {
      compensationFailed = true;
    }
  }

  if (completed.includes("payment")) {
    try {
      await recordStep("payment_refund", () =>
        act.refundPayment(order.orderId)
      );
    } catch {
      compensationFailed = true;
    }
  }

  if (compensationFailed) {
    return {
      orderId: order.orderId,
      status: "failed",
      code: "compensation_failed",
      trace,
    };
  }

  return {
    orderId: order.orderId,
    status: completed.length > 0 ? "compensated" : "failed",
    code: failure ?? "activity_failed",
    trace,
  };
}
