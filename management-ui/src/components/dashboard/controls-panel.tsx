"use client";

import { useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Panel } from "@/components/ui-custom/panel";
import { postSignal } from "@/lib/actions";
import type { ProxyControlState } from "@/lib/types";

type HardAction = "restart" | "shutdown";

const HARD_COPY: Record<HardAction, { title: string; body: string; verb: string }> = {
  restart: {
    title: "Restart the proxy process?",
    body: "The proxy will acknowledge over Temporal, exit gracefully, and its supervisor will relaunch it. In-flight deliveries are retried by Temporal — nothing is lost.",
    verb: "Restart",
  },
  shutdown: {
    title: "Shut the proxy process down?",
    body: "The proxy will exit gracefully and STAY DOWN until someone relaunches it on-site. Pending deliveries wait durably in Temporal. Prefer DISABLE for a reversible off-switch.",
    verb: "Shut down",
  },
};

export function ControlsPanel({
  state,
  onActed,
}: {
  state: ProxyControlState;
  onActed: () => void;
}) {
  const [busy, setBusy] = useState<string | null>(null);
  const [confirming, setConfirming] = useState<HardAction | null>(null);

  const act = async (action: string, label: string) => {
    setBusy(action);
    try {
      await postSignal(action);
      toast.success(`${label} signal sent`, {
        description: "Delivered to the control workflow — the proxy picks it up on its next poll.",
      });
      onActed();
    } catch (e) {
      toast.error(`${label} failed`, { description: e instanceof Error ? e.message : String(e) });
    } finally {
      setBusy(null);
      setConfirming(null);
    }
  };

  const lifecyclePending = (state.lifecycleCommand ?? "NONE") !== "NONE";

  return (
    <Panel legend="Controls">
      <div className="flex flex-col gap-4">
        <div>
          <div className="rule-label mb-2">soft · data plane</div>
          <div className="flex gap-2">
            <Button
              className="btn-hard flex-1 font-mono text-[11px] tracking-[0.12em] uppercase"
              variant={state.enabled ? "secondary" : "default"}
              disabled={state.enabled || busy !== null}
              onClick={() => act("enable", "ENABLE")}
            >
              Enable
            </Button>
            <Button
              className="btn-hard flex-1 font-mono text-[11px] tracking-[0.12em] uppercase"
              variant={state.enabled ? "default" : "secondary"}
              disabled={!state.enabled || busy !== null}
              onClick={() => act("disable", "DISABLE")}
            >
              Disable
            </Button>
          </div>
          <p className="mt-2 text-[11px] leading-snug text-ink-faint">
            Stops listeners and pauses outbound; the control poller stays connected, so it&apos;s
            instantly reversible.
          </p>
        </div>

        <div>
          <div className="rule-label mb-2">hard · process</div>
          <div className="flex gap-2">
            <Button
              className="btn-hard flex-1 font-mono text-[11px] tracking-[0.12em] uppercase"
              variant="outline"
              disabled={busy !== null || lifecyclePending}
              onClick={() => setConfirming("restart")}
            >
              Restart
            </Button>
            <Button
              className="btn-hard flex-1 font-mono text-[11px] tracking-[0.12em] uppercase"
              variant="destructive"
              disabled={busy !== null || lifecyclePending}
              onClick={() => setConfirming("shutdown")}
            >
              Shutdown
            </Button>
          </div>
          {lifecyclePending ? (
            <p className="readout mt-2 text-[11px] font-medium text-signal">
              {state.lifecycleCommand} pending — waiting for the proxy to acknowledge…
            </p>
          ) : state.applied?.supervised === false ? (
            <p className="readout mt-2 border border-warn/40 bg-warn/10 px-2 py-1.5 text-[11px] leading-snug text-warn">
              no supervisor detected — RESTART will exit and stay down. Run the proxy with
              `just run-proxy-managed` (or a service unit) to make restarts come back.
            </p>
          ) : (
            <p className="mt-2 text-[11px] leading-snug text-ink-faint">
              Rides Temporal like everything else — no management port on the proxy.
            </p>
          )}
        </div>
      </div>

      <Dialog open={confirming !== null} onOpenChange={(open) => !open && setConfirming(null)}>
        <DialogContent className="border-ink">
          {confirming && (
            <>
              <DialogHeader>
                <DialogTitle className="font-mono text-sm tracking-[0.08em] uppercase">
                  {HARD_COPY[confirming].title}
                </DialogTitle>
                <DialogDescription className="text-[13px] leading-relaxed">
                  {HARD_COPY[confirming].body}
                </DialogDescription>
              </DialogHeader>
              {confirming === "restart" && state.applied?.supervised === false && (
                <p className="readout border border-err/40 bg-err/10 px-3 py-2 text-[12px] leading-snug text-err">
                  The proxy reports NO supervisor — it will exit and nothing will relaunch
                  it. Someone on-site (or you, locally) must start it again. Use{" "}
                  <span className="font-semibold">just run-proxy-managed</span> for
                  restartable runs.
                </p>
              )}
              <DialogFooter>
                <Button variant="secondary" className="btn-hard" onClick={() => setConfirming(null)}>
                  Cancel
                </Button>
                <Button
                  variant={confirming === "shutdown" ? "destructive" : "default"}
                  className="btn-hard"
                  disabled={busy !== null}
                  onClick={() => act(confirming, HARD_COPY[confirming].verb.toUpperCase())}
                >
                  {HARD_COPY[confirming].verb}
                </Button>
              </DialogFooter>
            </>
          )}
        </DialogContent>
      </Dialog>
    </Panel>
  );
}
