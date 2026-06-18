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

# ANSI colors for better output
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
RESET = "\033[0m"

def log(message, color=BLUE):
    print(f"{color}[*] {message}{RESET}")

def run_command(command, check=True, capture_output=False):
    """Utility to run shell commands."""
    try:
        result = subprocess.run(
            command, 
            check=check, 
            text=True, 
            capture_output=capture_output
        )
        return result
    except subprocess.CalledProcessError as e:
        log(f"Command failed: {' '.join(command)}", RED)
        if check:
            sys.exit(1)
        return e

def check_docker():
    """Verify if docker is installed and running."""
    try:
        run_command(["docker", "info"], check=False, capture_output=True)
    except Exception:
        log("Docker is not running or not installed. Please start Docker first.", RED)
        sys.exit(1)

def start_services(detach=True):
    """Start all infrastructure and API services."""
    log("Starting all services via Docker Compose...")
    cmd = ["docker", "compose", "up", "--build"]
    if detach:
        cmd.append("-d")
    run_command(cmd)
    
    if detach:
        log("Services are starting in the background.", GREEN)
        log("Run 'python main.py logs' to see the output.", YELLOW)
    else:
        log("Services stopped.", YELLOW)

def stop_services():
    """Stop and remove all containers."""
    log("Stopping all services...")
    run_command(["docker", "compose", "down"])
    log("Services stopped.", GREEN)

def run_pipeline():
    """Run the data generation and ETL pipeline."""
    log("Starting Data Pipeline (Generation -> ETL -> Analytics)...")
    
    scripts = [
        ("Data Preparation", "src/producers/Generate_Real_Routes.py"),
        ("Batch ETL", "src/jobs/casablanca_batch_etl.py"),
        ("Analytics & KPIs", "src/jobs/mobility_kpi_analyzer.py")
    ]
    
    for name, path in scripts:
        if os.path.exists(path):
            log(f"Running {name}...", YELLOW)
            # Run in a separate process to avoid env pollution
            run_command([sys.executable, path])
        else:
            log(f"Skip: {path} not found.", RED)
    
    log("Pipeline execution finished.", GREEN)

def wait_for_services(timeout=120):
    """Wait for critical services (like API) to be ready."""
    log("Waiting for services to be ready (this may take a minute)...", YELLOW)
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Check if API is responding or at least running
            result = subprocess.run(["docker", "compose", "ps", "api"], capture_output=True, text=True)
            if "running" in result.stdout.lower():
                # Extra buffer for Kafka/Cassandra initialization
                time.sleep(15)
                log("Infrastructure is ready!", GREEN)
                return True
        except Exception:
            pass
        time.sleep(5)
    log("Timeout waiting for services. Proceeding with pipeline...", RED)
    return False

def show_logs(service=None):
    """Tail logs for all or specific services."""
    cmd = ["docker", "compose", "logs", "-f"]
    if service:
        cmd.append(service)
    try:
        run_command(cmd)
    except KeyboardInterrupt:
        print("\nExiting logs...")

def show_status():
    """Show status of all containers."""
    run_command(["docker", "compose", "ps"])

def main():
    parser = argparse.ArgumentParser(description="TaaSim Project Management CLI")
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
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

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
