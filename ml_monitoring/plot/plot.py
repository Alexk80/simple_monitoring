import os
import time

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

LOG_FILE = os.getenv("METRIC_LOG_PATH", "/app/logs/metric_log.csv")
PLOT_FILE = os.getenv("ERROR_PLOT_PATH", "/app/logs/error_distribution.png")
PLOT_UPDATE_SECONDS = int(os.getenv("PLOT_UPDATE_SECONDS", "5"))


def build_plot(df: pd.DataFrame) -> None:
    errors = df["absolute_error"].dropna()

    if errors.empty:
        return

    bins = min(30, max(10, len(errors) // 3))

    plt.figure(figsize=(10, 6))
    plt.hist(errors, bins=bins, color="royalblue", edgecolor="black", alpha=0.85)
    plt.title("Distribution of absolute error")
    plt.xlabel("absolute_error")
    plt.ylabel("count")
    plt.grid(axis="y", alpha=0.2)
    plt.tight_layout()
    plt.savefig(PLOT_FILE)
    plt.close()


def main() -> None:
    os.makedirs(os.path.dirname(PLOT_FILE), exist_ok=True)

    print("[plot] Plot service started.")

    while True:
        try:
            if not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0:
                print("[plot] metric_log.csv not found yet, waiting...")
                time.sleep(PLOT_UPDATE_SECONDS)
                continue

            df = pd.read_csv(LOG_FILE)

            if df.empty or "absolute_error" not in df.columns:
                print("[plot] no metric data yet, waiting...")
                time.sleep(PLOT_UPDATE_SECONDS)
                continue

            build_plot(df)
            print(f"[plot] updated plot: {PLOT_FILE}")

        except pd.errors.EmptyDataError:
            print("[plot] CSV is empty, waiting...")
        except Exception as exc:
            print(f"[plot] plotting error: {exc}")

        time.sleep(PLOT_UPDATE_SECONDS)


if __name__ == "__main__":
    main()