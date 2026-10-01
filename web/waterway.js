const { api: lfApi, setMessage: lfSetMessage, withBusy: lfWithBusy } = window.LockFlow;
const waterwayMessage = document.querySelector("#waterway-message");
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
const describe = (occupation) => `fleet [${occupation.fleet?.join(", ") || "?"}] ${occupation.phase} ${occupation.chamber_id} ${occupation.direction} since ${new Date(occupation.acquired_at).toLocaleTimeString()}`;

async function loadWaterway() {
  const status = await lfApi("/api/waterway");
  const directions = Object.fromEntries((status.directions || []).map((entry) => [entry.direction, entry]));
  for (const direction of ["upstream", "downstream"]) {
    const entry = directions[direction];
    document.querySelector(`#waterway-${direction}`).textContent = entry?.blocked ? "Blocked" : "Clear";
  }
  document.querySelector("#waterway-count").textContent = String(status.occupations?.length || 0);
  const reasons = (status.directions || []).filter((entry) => entry.blocked)
    .map((entry) => `${entry.direction} blocked: ${entry.blockers.map(describe).join("; ")}`);
  lfSetMessage(document.querySelector("#waterway-blocked"), reasons.join("\n"));
  const rows = (status.occupations || []).map((occupation) => `<tr><td>${esc(occupation.fleet?.join(", "))}</td><td>${esc(occupation.chamber_id)}</td><td>${esc(occupation.direction)}</td><td>${esc(occupation.phase)}</td><td>${new Date(occupation.acquired_at).toLocaleTimeString()}</td><td><button class="danger" data-release="${occupation.id}">Release</button></td></tr>`);
  document.querySelector("#waterway").innerHTML = rows.join("") || '<tr><td colspan="6" class="muted">Waterway free</td></tr>';
  document.querySelectorAll("[data-release]").forEach((button) => button.addEventListener("click", () => releaseOccupation(button.dataset.release, button)));
}

async function releaseOccupation(occupationID, button) {
  await lfWithBusy(button, async () => {
    try {
      await lfApi(`/api/waterway/occupations/${occupationID}/release`, { method: "POST", body: JSON.stringify({ reason: "operator release" }) });
      lfSetMessage(waterwayMessage, "");
      await loadWaterway();
    } catch (error) { lfSetMessage(waterwayMessage, error.message); }
  });
}

loadWaterway().catch((error) => lfSetMessage(waterwayMessage, error.message));
setInterval(() => loadWaterway().catch(() => {}), 5000);
