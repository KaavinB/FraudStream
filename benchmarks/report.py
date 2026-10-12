import json
import os
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", str(Path(".cache/matplotlib").resolve()))
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main():
    directory = Path("benchmarks/results")
    directory.mkdir(parents=True, exist_ok=True)
    experiments = []
    for path in sorted(directory.glob("*.json")):
        if path.name == "report.json":
            continue
        result = json.loads(path.read_text())
        if "experiment_id" not in result:
            continue
        kind = result["experiment_id"].split("-")[0]
        # Dashboard gets summaries; full per-event raw data stays in the original artifact.
        overview = {k: v for k, v in result.items() if not k.endswith("raw_ms")}
        overview["kind"], overview["raw_artifact"] = kind, str(path)
        if "trials" in overview:
            overview["trials"] = [
                {k: v for k, v in t.items() if k != "raw"} for t in overview["trials"]
            ]
        if "batch_compute" in overview:
            overview["batch_compute"] = [
                {k: v for k, v in b.items() if k != "raw_ms"} for b in overview["batch_compute"]
            ]
        experiments.append(overview)
        if result.get("status") == "executed" and kind == "local":
            fig, axes = plt.subplots(1, 2, figsize=(10, 4))
            axes[0].hist(result["single_compute_raw_ms"], bins=50, color="#17695d")
            axes[0].set(
                xlabel="Model compute latency (ms)",
                ylabel="Observed predictions",
                title="Actual calibrated model, CPU",
            )
            batches = result["batch_compute"]
            axes[1].plot(
                [r["batch_size"] for r in batches],
                [r["compute_predictions_per_second"] for r in batches],
                marker="o",
                color="#17695d",
            )
            axes[1].set(
                xlabel="Batch size (no queue delay)",
                ylabel="Compute predictions / sec",
                title="Component-only batch capacity",
            )
            fig.tight_layout()
            fig.savefig(directory / (result["experiment_id"] + ".png"), dpi=160)
            plt.close(fig)
        if result.get("status") == "executed" and kind == "pipeline":
            rows = result["trials"]
            closed = result.get("mode", "closed") == "closed"
            fig, axes = plt.subplots(1, 2 if closed else 1, figsize=(10 if closed else 5, 4))
            throughput = axes[0] if closed else axes
            throughput.scatter(
                [r["concurrency"] for r in rows],
                [r["completed_unique_decisions_per_second"] for r in rows],
            )
            throughput.set(xlabel="Clients", ylabel="Completed unique decisions/sec")
            if closed:
                axes[1].scatter(
                    [r["concurrency"] for r in rows],
                    [r["client_post_ack_to_observed_decision"]["p95_ms"] for r in rows],
                )
                axes[1].set(xlabel="Clients", ylabel="Post-ack durable observation p95 (ms)")
            fig.tight_layout()
            fig.savefig(directory / (result["experiment_id"] + ".png"), dpi=160)
            plt.close(fig)
    model_path = Path("ml/artifacts/model-info.json")
    model = json.loads(model_path.read_text()) if model_path.exists() else None
    if model:
        fig, ax = plt.subplots(figsize=(6, 4))
        for name, row in model["models"].items():
            curve = row["test"]["pr_curve"]
            ax.plot(
                curve["recall"],
                curve["precision"],
                label=f"{name}: AP={row['test']['average_precision']:.3f}",
            )
        ax.set(
            xlabel="Recall",
            ylabel="Precision",
            title="Chronologically held-out synthetic transactions",
        )
        ax.legend()
        fig.tight_layout()
        fig.savefig(directory / "model-comparison.png", dpi=160)
        plt.close(fig)
    report = {
        "experiments": experiments,
        "model_report": "ml/artifacts/model-info.json" if model else None,
        "unmeasured": [
            "infrastructure outages (Redis/PostgreSQL/inference unavailable) under load",
            "multi-broker Kafka or replicated PostgreSQL/Redis availability",
            "open-loop load at a fixed arrival rate",
            "feature freshness distribution (only worker histogram sums are exported)",
        ],
    }
    (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    lines = [
        "# Executed benchmark report",
        "",
        "Raw artifacts include individual measurements and conditions. Blocked runs are not successful benchmarks.",
        "",
    ]
    for experiment in experiments:
        lines.extend(
            [
                f"## {experiment['experiment_id']}",
                "",
                f"Status: {experiment['status']}; artifact: `{experiment['raw_artifact']}`.",
                "",
            ]
        )
        if experiment.get("label"):
            lines.extend([f"**{experiment['label']}**", ""])
        if experiment.get("single_compute"):
            lines.extend(
                [
                    f"CPU model compute p95: {experiment['single_compute']['p95_ms']:.3f} ms; in-process FastAPI p95: {experiment['in_process_fastapi']['p95_ms']:.3f} ms.",
                    "",
                    "These measure actual software on synthetic transactions. They exclude Kafka, Redis, PostgreSQL, and TCP.",
                    "",
                ]
            )
        if experiment["kind"] == "pipeline" and experiment.get("trials"):
            lines.append(
                f"Mode: {experiment.get('mode', 'closed')}. {experiment.get('workload', '')}"
            )
            lines.append("")
            lines.append("| Clients | Decisions/sec | Post-ack p95 (ms) | Errors |")
            lines.append("|---:|---:|---:|---:|")
            for t in experiment["trials"]:
                observed = t.get("client_post_ack_to_observed_decision", {}).get("p95_ms")
                p95 = f"{observed:.0f}" if observed is not None else "—"
                lines.append(
                    f"| {t['concurrency']} | {t['completed_unique_decisions_per_second']:.1f} | {p95} | {t['error_count']} |"
                )
            lines.append("")
        if experiment["kind"] == "features" and experiment.get("status") == "executed":
            lines.append(
                "| Implementation | Entities | Events/sec | Lookup p95 (ms) | Update p95 (ms) |"
            )
            lines.append("|---|---:|---:|---:|---:|")
            for t in experiment["trials"]:
                lines.append(
                    f"| {t['implementation']} | {t['entities']} | {t['events_per_second']:.1f} | {t['lookup']['p95_ms']:.2f} | {t['update']['p95_ms']:.2f} |"
                )
            lines.append("")
        if experiment["kind"] == "reliability" and experiment.get("scenarios"):
            lines.append("| Crash point | Recovery (s) | Duplicate decisions | Missing decisions |")
            lines.append("|---|---:|---:|---:|")
            for sc in experiment["scenarios"]:
                lines.append(
                    f"| {sc['scenario']} | {sc['recovery_seconds']:.1f} | {sc['duplicate_durable_decisions']} | {sc['missing_durable_decisions']} |"
                )
            lines.append("")
        if experiment.get("error"):
            lines.extend(["Blocking evidence: " + experiment["error"], ""])
    Path("docs/BENCHMARK_RESULTS.md").write_text("\n".join(lines) + "\n")
    print(f"Generated {len(experiments)} experiment summaries and plots in {directory}")


if __name__ == "__main__":
    main()
