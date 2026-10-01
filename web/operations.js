const { api, shortID, setMessage, withBusy } = window.LockFlow;
const form = document.querySelector("#plan-form");
const message = document.querySelector("#plan-message");

const convoyNames = (vessels = []) => vessels.map((vessel) => vessel.name || vessel.call_sign).join(", ") || "-";

function renderWaterways(board) {
  const rows = (board.waterways || []).map((reach) => {
    const occupied = (reach.occupants || []).map((occupant) => {
      const req = occupant.request || {};
      return `<div><strong>${req.chamber_id || "-"}</strong> · ${req.heading || "-"} · ${convoyNames(req.vessels)} <span class="muted">[${shortID(req.cycle_id)}]</span></div>`;
    }).join("") || '<span class="muted">— clear —</span>';
    const waiting = (reach.waiting || []).map((item) => {
      const req = item.request || {};
      return `<div>⛔ <strong>${req.chamber_id || "-"}</strong> · ${req.heading || "-"} · ${convoyNames(req.vessels)}<div class="muted">${item.reason || "waiting for clearance"}</div></div>`;
    }).join("") || '<span class="muted">none</span>';
    const status = reach.clear_to_go
      ? '<span class="status">Clear</span>'
      : `<span class="status ${reach.waiting && reach.waiting.length ? "warning" : ""}">Occupied · ${reach.heading || ""}</span>`;
    return `<tr><td>${reach.waterway}</td><td>${status}</td><td>${occupied}</td><td>${waiting}</td></tr>`;
  });
  document.querySelector("#waterways").innerHTML = rows.join("") || '<tr><td colspan="4" class="muted">No shared reaches</td></tr>';
}

async function refresh() {
  const [plans, chambers, health, water] = await Promise.all([api("/api/plans"), api("/api/chambers"), api("/healthz"), api("/api/waterspace")]);
  document.querySelector("#health").textContent = health.status === "ok" ? "Devices online" : "Attention required";
  renderWaterways(water);
  const rows = Object.entries(plans).flatMap(([chamber, items]) => items.map((plan) => `<tr><td>${chamber}</td><td>${shortID(plan.id)}</td><td>${plan.direction}</td><td>${plan.vessels.length}</td><td>${new Date(plan.created_at).toLocaleTimeString()}</td></tr>`));
  document.querySelector("#plans").innerHTML = rows.join("") || '<tr><td colspan="5" class="muted">No active plans</td></tr>';
  const current = chambers.find((item) => item.cycle);
  document.querySelector("#metric-chamber").textContent = current?.chamber_id || "Idle";
  document.querySelector("#metric-stage").textContent = current?.cycle?.stage || "-";
  document.querySelector("#metric-cycle").textContent = shortID(current?.cycle?.id);
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = form.querySelector("button");
  await withBusy(button, async () => {
    try {
      const result = await api("/api/plans", { method: "POST", body: JSON.stringify({
        chamber_id: document.querySelector("#chamber").value,
        direction: document.querySelector("#direction").value,
        vessels: [{ name: document.querySelector("#vessel").value, call_sign: document.querySelector("#call-sign").value, length_m: Number(document.querySelector("#length").value) }],
      }) });
      setMessage(message, `Plan ${shortID(result.plan.id)} admitted to ${result.plan.chamber_id}.`);
      await refresh();
    } catch (error) { setMessage(message, error.message); }
  });
});

refresh().catch((error) => setMessage(message, error.message));
setInterval(() => refresh().catch(() => {}), 5000);
