let previewPayload = null;
let mergedFile = null;

function statCard(k, v) {
  return `<div class="stat"><div class="k">${k}</div><div class="v">${v ?? "—"}</div></div>`;
}

document.getElementById("previewForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  document.getElementById("messages").textContent = "";
  const form = new FormData(e.target);
  const res = await fetch("/api/tools/fit-hr-merge/preview", { method: "POST", body: form });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById("messages").textContent = data.error || "Preview failed";
    return;
  }
  previewPayload = data;
  const fit = data.fit_summary;
  const apple = data.apple_summary;
  document.getElementById("previewGrid").innerHTML = [
    statCard("FIT Start", fit.start),
    statCard("FIT End", fit.end),
    statCard("FIT Duration", fit.duration),
    statCard("FIT Records", fit.sample_count),
    statCard("FIT has HR", fit.existing_hr ? "Yes" : "No"),
    statCard("Apple points", apple.point_count),
    statCard("Apple first", apple.first_timestamp),
    statCard("Apple last", apple.last_timestamp),
    statCard("Estimated overlap", data.estimated_overlap_points),
  ].join("");
  document.getElementById("warnings").innerHTML = (data.warnings || []).map(w => `<li>${w}</li>`).join("");
  document.getElementById("previewSection").classList.remove("hidden");
  document.getElementById("optionsSection").classList.remove("hidden");
});

document.getElementById("runMerge").addEventListener("click", async () => {
  if (!previewPayload) {
    document.getElementById("messages").textContent = "Run preview first.";
    return;
  }
  const payload = {
    ...previewPayload,
    tolerance_seconds: Number(document.getElementById("tolerance").value),
    overwrite_existing: document.getElementById("overwrite").checked,
    ignore_implausible: document.getElementById("ignoreImplausible").checked,
    min_hr: Number(document.getElementById("minHr").value),
    max_hr: Number(document.getElementById("maxHr").value),
    output_name: document.getElementById("outputName").value,
    source_type: document.querySelector("select[name=source_type]").value,
  };
  const res = await fetch("/api/tools/fit-hr-merge/run", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!res.ok) {
    document.getElementById("messages").textContent = data.error || "Merge failed";
    return;
  }
  mergedFile = data;
  const r = data.report;
  document.getElementById("resultGrid").innerHTML = [
    statCard("Apple points parsed", r.total_apple_hr_points),
    statCard("FIT records", r.total_fit_records),
    statCard("HR points matched", r.hr_points_matched),
    statCard("HR points written", r.hr_points_written),
    statCard("Coverage %", r.coverage_pct),
    statCard("First HR used", r.first_hr_timestamp_used),
    statCard("Last HR used", r.last_hr_timestamp_used),
    statCard("Gaps > 30s", r.gaps_over_30s),
    statCard("Preserved existing HR", r.preserved_existing_hr),
  ].join("");
  document.getElementById("resultSection").classList.remove("hidden");
});

document.getElementById("downloadBtn").addEventListener("click", () => {
  if (!mergedFile) return;
  const bytes = atob(mergedFile.merged_fit_b64);
  const arr = new Uint8Array(bytes.length);
  for (let i = 0; i < bytes.length; i += 1) arr[i] = bytes.charCodeAt(i);
  const blob = new Blob([arr], { type: "application/octet-stream" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = mergedFile.filename;
  a.click();
  URL.revokeObjectURL(a.href);
});
