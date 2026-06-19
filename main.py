#!/usr/bin/env python3
"""
TaaSim Unified Entrypoint
-------------------------
This script provides a single command-line interface to manage all services,
data pipelines, and development tasks for the TaaSim project.
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime

# ── ANSI colors ──────────────────────────────────────────────────────────────
BLUE   = "\033[94m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

# ── Logging helpers ──────────────────────────────────────────────────────────
def _ts():
    """Return a human-readable timestamp."""
    return datetime.now().strftime("%H:%M:%S")

def log(message, color=BLUE):
    print(f"{DIM}[{_ts()}]{RESET} {color}[*] {message}{RESET}")

def log_step(step_num, total, message):
    print(f"{DIM}[{_ts()}]{RESET} {CYAN}{BOLD}[Step {step_num}/{total}]{RESET} {message}")

def log_success(message):
    print(f"{DIM}[{_ts()}]{RESET} {GREEN}  ✔ {message}{RESET}")

def log_warn(message):
    print(f"{DIM}[{_ts()}]{RESET} {YELLOW}  ⚠ {message}{RESET}")

def log_error(message):
    print(f"{DIM}[{_ts()}]{RESET} {RED}  ✖ {message}{RESET}")

def log_info(message):
    print(f"{DIM}[{_ts()}]{RESET} {DIM}    → {message}{RESET}")

def banner(text):
    width = 60
    print()
    print(f"{CYAN}{'═' * width}")
    print(f"  {BOLD}{text}{RESET}{CYAN}")
    print(f"{'═' * width}{RESET}")
    print()


# ── Command runner ───────────────────────────────────────────────────────────
def run_command(command, check=True, capture_output=False):
    """Run a shell command, log it, and return the result."""
    cmd_str = " ".join(command)
    log_info(f"Running: {cmd_str}")
    try:
        result = subprocess.run(
            command,
            check=check,
            text=True,
            capture_output=capture_output
        )
        return result
    except subprocess.CalledProcessError as e:
        log_error(f"Command failed (exit {e.returncode}): {cmd_str}")
        if capture_output and e.stderr:
            for line in e.stderr.strip().splitlines()[:5]:
                log_info(f"stderr: {line}")
        if check:
            sys.exit(1)
        return e


# ── Docker check ─────────────────────────────────────────────────────────────
def check_docker():
    """Verify if docker is installed and running."""
    log("Checking Docker availability...")
    result = run_command(["docker", "info"], check=False, capture_output=True)
    if isinstance(result, subprocess.CalledProcessError) or result.returncode != 0:
        log_error("Docker is not running or not installed. Please start Docker first.")
        sys.exit(1)
    log_success("Docker daemon is running.")


# ── Service management ───────────────────────────────────────────────────────
def start_services(detach=True):
    """Start all infrastructure and API services."""
    banner("STARTING ALL SERVICES")
    log("Building and launching containers via Docker Compose...")
    cmd = ["docker", "compose", "up", "--build"]
    if detach:
        cmd.append("-d")
    t0 = time.time()
    run_command(cmd)
    elapsed = time.time() - t0

    if detach:
        log_success(f"All containers launched in {elapsed:.1f}s.")
        log_info("Run 'python main.py logs' to tail the output.")
    else:
        log("Services stopped.", YELLOW)


def stop_services():
    """Stop and remove all containers."""
    banner("STOPPING ALL SERVICES")
    log("Tearing down containers...")
    run_command(["docker", "compose", "down"])
    log_success("All services stopped and removed.")


# ── Wait for readiness ───────────────────────────────────────────────────────
def wait_for_services(timeout=180):
    """Wait for critical services to be healthy before running the pipeline."""
    banner("WAITING FOR SERVICES TO BE READY")

    services_to_check = [
        ("kafka",      "taasim-kafka"),
        ("cassandra",  "taasim-cassandra"),
        ("minio",      "taasim-minio"),
        ("api",        "taasim-api"),
        ("flink-jm",   "taasim-flink-jm"),
        ("grafana",    "taasim-grafana"),
    ]

    log(f"Polling container health (timeout {timeout}s)...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        all_ready = True
        for label, container in services_to_check:
            result = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container],
                capture_output=True, text=True
            )
            running = result.stdout.strip() == "true"
            if not running:
                all_ready = False

        if all_ready:
            elapsed = time.time() - start_time
            log_success(f"All {len(services_to_check)} core containers are running ({elapsed:.0f}s).")
            # Extra buffer for Cassandra / Kafka initialization
            log_info("Giving services 20s extra to finish internal initialization...")
            time.sleep(20)
            log_success("Infrastructure is ready!")
            return True

        time.sleep(5)

    log_warn("Timeout reached – some containers may not be ready yet.")
    log_info("Proceeding with pipeline anyway...")
    return False


# ── Full pipeline ────────────────────────────────────────────────────────────
TOTAL_PIPELINE_STEPS = 4

def run_pipeline():
    banner("RUNNING FULL DATA PIPELINE")
    pipeline_start = time.time()
    results = []  # (step_name, status, elapsed)

    # ── Step 1: Flink Streaming Jobs ─────────────────────────────────────
    log_step(1, TOTAL_PIPELINE_STEPS, "Submitting Flink Streaming Jobs")
    flink_jobs = [
        ("Job 1 – GPS Normalizer",     "/opt/flink/jobs/job1_gps_normalizer.py"),
        ("Job 2 – Demand Aggregator",  "/opt/flink/jobs/job2_demand_aggregator.py"),
        ("Job 3 – Trip Matcher",       "/opt/flink/jobs/job3_trip_matcher.py"),
    ]
    for name, path in flink_jobs:
        t0 = time.time()
        log_info(f"Submitting {name}...")
        result = run_command([
            "docker", "compose", "exec", "-d", "flink-jobmanager",
            "flink", "run", "--python", path
        ], check=False, capture_output=True)
        elapsed = time.time() - t0

        if result.returncode != 0:
            log_warn(f"{name} submission returned non-zero (may still start).")
            results.append((f"Flink: {name}", "WARN", elapsed))
        else:
            log_success(f"{name} submitted ({elapsed:.1f}s).")
            results.append((f"Flink: {name}", "OK", elapsed))
        time.sleep(3)

    # ── Step 2: Spark Batch Jobs ─────────────────────────────────────────
    log_step(2, TOTAL_PIPELINE_STEPS, "Running Spark Batch & ML Jobs")
    
    spark_scripts = [
        ("Batch ETL (Week 5)",             "src/jobs/casablanca_batch_etl.py"),
        ("Analytics & KPIs (Week 5)",      "src/jobs/mobility_kpi_analyzer.py"),
        ("ML Demand Forecast (Week 6)",    "src/jobs/train_demand_model.py"),
    ]
    for name, path in spark_scripts:
        t0 = time.time()
        log_info(f"Running {name}...")
        result = run_command([
            "docker", "compose", "exec", "api",
            "python", path
        ], check=False, capture_output=True)
        elapsed = time.time() - t0

        if result.returncode != 0:
            log_error(f"{name} failed after {elapsed:.1f}s.")
            results.append((f"Spark: {name}", "FAIL", elapsed))
        else:
            log_success(f"{name} completed ({elapsed:.1f}s).")
            results.append((f"Spark: {name}", "OK", elapsed))

    # ── Step 3: Start Continuous Producers ───────────────────────────────
    log_step(3, TOTAL_PIPELINE_STEPS, "Starting Continuous Kafka Producers")
    producers = [
        ("GPS Stream Producer",       "src/producers/ProducerGps.py"),
        ("Trip Requests Producer",    "src/producers/ProducerTrips.py"),
    ]
    for name, path in producers:
        t0 = time.time()
        log_info(f"Starting {name} in background...")
        result = run_command([
            "docker", "compose", "exec", "-d", "api",
            "python", path
        ], check=False, capture_output=True)
        elapsed = time.time() - t0

        if result.returncode != 0:
            log_warn(f"{name} could not start.")
            results.append((f"Producer: {name}", "WARN", elapsed))
        else:
            log_success(f"{name} started ({elapsed:.1f}s).")
            results.append((f"Producer: {name}", "OK", elapsed))

    # ── Step 4: Inject Demand Spike ──────────────────────────────────────
    log_step(4, TOTAL_PIPELINE_STEPS, "Injecting Demand Spike (Event Injector)")
    t0 = time.time()
    log_info("Sending 150 burst trip requests to Kafka...")
    result = run_command([
        "docker", "compose", "exec", "api",
        "python", "src/producers/event_injector.py"
    ], check=False, capture_output=True)
    elapsed = time.time() - t0

    if result.returncode != 0:
        log_error(f"Event Injector failed after {elapsed:.1f}s.")
        results.append(("Event Injector", "FAIL", elapsed))
    else:
        log_success(f"Demand spike injected ({elapsed:.1f}s).")
        results.append(("Event Injector", "OK", elapsed))

    # ── Summary Report ───────────────────────────────────────────────────
    total_elapsed = time.time() - pipeline_start
    banner("PIPELINE EXECUTION SUMMARY")

    print(f"  {'Task':<45} {'Status':<8} {'Time':>8}")
    print(f"  {'─' * 45} {'─' * 8} {'─' * 8}")
    for task_name, status, elapsed in results:
        if status == "OK":
            color = GREEN
        elif status == "WARN":
            color = YELLOW
        else:
            color = RED
        print(f"  {task_name:<45} {color}{status:<8}{RESET} {elapsed:>7.1f}s")

    print(f"\n  {BOLD}Total pipeline time: {total_elapsed:.1f}s{RESET}")

    ok_count   = sum(1 for _, s, _ in results if s == "OK")
    warn_count = sum(1 for _, s, _ in results if s == "WARN")
    fail_count = sum(1 for _, s, _ in results if s == "FAIL")
    print(f"  {GREEN}✔ {ok_count} passed{RESET}  {YELLOW}⚠ {warn_count} warnings{RESET}  {RED}✖ {fail_count} failed{RESET}")

    print()
    log_info("Grafana dashboard:  http://localhost:3000  (admin / admin)")
    log_info("TaaSim API:         http://localhost:8001")
    log_info("Flink UI:           http://localhost:8081")
    log_info("MinIO Console:      http://localhost:9001  (admin / password123)")
    log_info("Spark Master UI:    http://localhost:8080")
    print()


# ── Logs & status ────────────────────────────────────────────────────────────
def show_logs(service=None):
    """Tail logs for all or specific services."""
    cmd = ["docker", "compose", "logs", "-f"]
    if service:
        cmd.append(service)
    log(f"Tailing logs{' for ' + service if service else ''}... (Ctrl+C to exit)")
    try:
        run_command(cmd)
    except KeyboardInterrupt:
        print("\nExiting logs...")


def show_status():
    """Show status of all containers."""
    banner("SERVICE STATUS")
    run_command(["docker", "compose", "ps"])


# ── CLI ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="TaaSim Project Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py run        # Build, start, and run the full pipeline
  python main.py start      # Just start the Docker services
  python main.py pipeline   # Run pipeline (services must already be up)
  python main.py stop       # Tear everything down
  python main.py status     # Show container status
  python main.py logs api   # Tail logs for the API container
"""
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command (One single command)
    subparsers.add_parser("run", help="Start all services and run the data pipeline automatically")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start all services (Docker)")
    start_parser.add_argument("--foreground", "-f", action="store_true", help="Run in foreground")

    # Stop command
    subparsers.add_parser("stop", help="Stop all services")

    # Pipeline command
    subparsers.add_parser("pipeline", help="Run the data generation and ETL pipeline")

    # Logs command
    logs_parser = subparsers.add_parser("logs", help="Tail logs")
    logs_parser.add_argument("service", nargs="?", help="Optional service name to filter logs")

    # Status command
    subparsers.add_parser("status", help="Show status of services")

    # Default help
    if len(sys.argv) == 1:
        banner("TaaSim — Transport as a Service")
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    banner("TaaSim — Transport as a Service")
    check_docker()

    if args.command == "run":
        start_services(detach=True)
        wait_for_services()
        run_pipeline()
    elif args.command == "start":
        start_services(detach=not args.foreground)
    elif args.command == "stop":
        stop_services()
    elif args.command == "pipeline":
        run_pipeline()
    elif args.command == "logs":
        show_logs(args.service)
    elif args.command == "status":
        show_status()

if __name__ == "__main__":
    main()
