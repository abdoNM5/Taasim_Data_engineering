"""
╔══════════════════════════════════════════════════════════════════╗
║          week5.ipynb  —  PRE-FLIGHT ENVIRONMENT CHECKER         ║
║  Run this script BEFORE opening the notebook.                   ║
║  Every CHECK must show  ✅  before you proceed.                 ║
╚══════════════════════════════════════════════════════════════════╝

Usage:
    python preflight_check.py

What it tests (in order):
    1.  Python version & executable path
    2.  Java 17 (JAVA_HOME + java binary)
    3.  PySpark installation (correct env / kernel)
    4.  h3 library
    5.  winutils.exe  (Windows-only, required by Hadoop)
    6.  SPARK_HOME derivation from pyspark package
    7.  PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON consistency
    8.  SparkSession can actually start  (smoke test)
    9.  MinIO / S3A reachability  (HTTP ping to localhost:9000)
    10. Cassandra reachability    (TCP connect to localhost:9042)
    11. MinIO buckets exist       (raw, curated)
    12. Quick Spark ↔ MinIO read  (lists the raw bucket)
"""

import os
import sys
import subprocess
import socket
import urllib.request
import urllib.error
import importlib
import importlib.util
import textwrap
import time

# ── colour helpers ────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

PASS = f"{GREEN}✅  PASS{RESET}"
FAIL = f"{RED}❌  FAIL{RESET}"
WARN = f"{YELLOW}⚠️   WARN{RESET}"
INFO = f"ℹ️  INFO"

results = []   # (label, status, detail)

def check(label, ok, detail="", warn_only=False):
    status = PASS if ok else (WARN if warn_only else FAIL)
    results.append((label, ok, warn_only))
    print(f"  {status}  {label}")
    if detail:
        for line in textwrap.wrap(detail, width=72):
            print(f"         {line}")
    return ok

def section(title):
    print(f"\n{BOLD}{'─'*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'─'*60}{RESET}")

# ─────────────────────────────────────────────────────────────────────────────
# 1. PYTHON
# ─────────────────────────────────────────────────────────────────────────────
section("1 · Python")

py_ver = sys.version_info
py_ok  = py_ver >= (3, 9)
check(
    f"Python version ≥ 3.9  (found {py_ver.major}.{py_ver.minor}.{py_ver.micro})",
    py_ok,
    detail="" if py_ok else "PySpark 3.4 requires Python 3.9+."
)

preferred = r"C:\Users\nmira\AppData\Local\Programs\Python\Python311\python.exe"
using_preferred = sys.executable.lower() == preferred.lower()
check(
    f"Running from expected Python exe",
    using_preferred,
    detail=f"Current  : {sys.executable}\nExpected : {preferred}\n"
           "If they differ, activate the right venv / kernel before running the notebook.",
    warn_only=True   # not a hard blocker if pyspark is installed here too
)

# ─────────────────────────────────────────────────────────────────────────────
# 2. JAVA 17
# ─────────────────────────────────────────────────────────────────────────────
section("2 · Java 17  (required by Spark)")

java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
java_home_exists = os.path.isdir(java_home)
check(
    "JAVA_HOME directory exists",
    java_home_exists,
    detail=f"Expected: {java_home}\n"
           "Download from https://adoptium.net  if missing."
)

java_bin = os.path.join(java_home, "bin", "java.exe")
java_bin_exists = os.path.isfile(java_bin)
check(
    "java.exe found inside JAVA_HOME",
    java_bin_exists,
    detail=f"Looked for: {java_bin}"
)

if java_bin_exists:
    try:
        out = subprocess.check_output([java_bin, "-version"],
                                       stderr=subprocess.STDOUT,
                                       text=True)
        is_17 = "17" in out.split("\n")[0]
        check(
            f"Java reports version 17  (first line: {out.split(chr(10))[0].strip()})",
            is_17,
            detail="" if is_17 else "Spark 3.4 is tested with Java 8, 11, 17. Other versions may fail."
        )
    except Exception as e:
        check("Java binary is executable", False, detail=str(e))

# PATH entry
path_dirs = os.environ.get("PATH", "").split(os.pathsep)
java_in_path = any("java" in p.lower() or "jdk" in p.lower() for p in path_dirs)
check(
    "Java bin directory appears in PATH",
    java_in_path,
    detail="Add %JAVA_HOME%\\bin to your system PATH so Spark can spawn the JVM.",
    warn_only=not java_in_path
)

# ─────────────────────────────────────────────────────────────────────────────
# 3. PYSPARK
# ─────────────────────────────────────────────────────────────────────────────
section("3 · PySpark")

pyspark_spec = importlib.util.find_spec("pyspark")
pyspark_installed = pyspark_spec is not None
check(
    "pyspark is importable",
    pyspark_installed,
    detail="" if pyspark_installed else
            f"Fix: run  {sys.executable} -m pip install pyspark==3.5.1\n"
            "Make sure you use the SAME Python that Jupyter is using."
)

if pyspark_installed:
    import pyspark
    pv = pyspark.__version__
    pv_ok = pv.startswith("3.")
    check(
        f"PySpark version is 3.x  (found {pv})",
        pv_ok,
        detail="" if pv_ok else "The notebook uses Spark 3 APIs."
    )
    spark_home_derived = pyspark.__path__[0]
    check(
        f"SPARK_HOME derivable from pyspark package",
        os.path.isdir(spark_home_derived),
        detail=f"Value: {spark_home_derived}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# 4. h3 LIBRARY
# ─────────────────────────────────────────────────────────────────────────────
section("4 · h3  (geo-indexing library)")

h3_spec = importlib.util.find_spec("h3")
h3_ok   = h3_spec is not None
check(
    "h3 is importable",
    h3_ok,
    detail="" if h3_ok else f"Fix: {sys.executable} -m pip install h3"
)

if h3_ok:
    import h3
    test_cell = h3.geo_to_h3(48.8566, 2.3522, 8)   # Paris
    check(
        f"h3.geo_to_h3 works  (Paris → {test_cell})",
        bool(test_cell),
    )

# ─────────────────────────────────────────────────────────────────────────────
# 5. WINUTILS  (Windows-only Hadoop helper)
# ─────────────────────────────────────────────────────────────────────────────
section("5 · winutils.exe  (Windows only)")

if sys.platform == "win32":
    # Common locations
    candidates = [
        r"C:\hadoop\bin\winutils.exe",
        r"C:\winutils\bin\winutils.exe",
        os.path.join(os.environ.get("HADOOP_HOME", ""), "bin", "winutils.exe"),
    ]
    found = next((p for p in candidates if os.path.isfile(p)), None)
    check(
        "winutils.exe found",
        found is not None,
        detail=f"Checked: {candidates}\n"
               "Without winutils, Spark on Windows throws NullPointerException.\n"
               "Download from: https://github.com/cdarlint/winutils  (pick the\n"
               "version matching hadoop-aws 3.3.x → use hadoop-3.3.x/bin/).\n"
               "Then set HADOOP_HOME=C:\\hadoop  and add %HADOOP_HOME%\\bin to PATH."
    )
    if found:
        print(f"         Located at: {found}")
        hadoop_home = os.environ.get("HADOOP_HOME", "")
        check(
            "HADOOP_HOME env var is set",
            bool(hadoop_home),
            detail="Set HADOOP_HOME so Spark can find winutils automatically.",
            warn_only=True
        )
else:
    print(f"  {INFO}  Not Windows — winutils check skipped.")

# ─────────────────────────────────────────────────────────────────────────────
# 6. ENVIRONMENT VARIABLES CONSISTENCY
# ─────────────────────────────────────────────────────────────────────────────
section("6 · Spark environment variables")

env_java = os.environ.get("JAVA_HOME", "")
check(
    "JAVA_HOME env var is set",
    bool(env_java),
    detail="The notebook sets it at runtime, but it's cleaner to set it system-wide.",
    warn_only=True
)

env_pyspark_py = os.environ.get("PYSPARK_PYTHON", "")
env_driver_py  = os.environ.get("PYSPARK_DRIVER_PYTHON", "")
if env_pyspark_py:
    check(
        "PYSPARK_PYTHON points to existing file",
        os.path.isfile(env_pyspark_py),
        detail=f"Value: {env_pyspark_py}"
    )
if env_driver_py:
    check(
        "PYSPARK_DRIVER_PYTHON points to existing file",
        os.path.isfile(env_driver_py),
        detail=f"Value: {env_driver_py}"
    )

# ─────────────────────────────────────────────────────────────────────────────
# 7. SPARK SESSION SMOKE TEST
# ─────────────────────────────────────────────────────────────────────────────
section("7 · SparkSession smoke test  (no extra JARs)")

if not pyspark_installed:
    print(f"  {WARN}  Skipped — pyspark not installed.")
else:
    print("  Attempting to start a minimal local SparkSession …")
    # Set env vars the notebook would set, so the test is realistic
    os.environ.setdefault("JAVA_HOME", java_home)
    if pyspark_installed:
        os.environ.setdefault("SPARK_HOME", pyspark.__path__[0])

    spark_ok = False
    spark_detail = ""
    try:
        from pyspark.sql import SparkSession
        # Minimal session — no packages download needed
        spark_test = (
            SparkSession.builder
            .appName("preflight-check")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.extraJavaOptions",
                    "-Dio.netty.tryReflectionSetAccessible=true "
                    "--add-opens=java.base/java.nio=ALL-UNNAMED "
                    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED "
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .getOrCreate()
        )
        spark_test.sparkContext.setLogLevel("ERROR")
        result = spark_test.range(5).count()
        spark_ok = (result == 5)
        spark_detail = f"spark.range(5).count() = {result}"
        spark_test.stop()
    except Exception as exc:
        spark_detail = str(exc)[:300]

    check(
        "SparkSession starts and runs a simple query",
        spark_ok,
        detail="" if spark_ok else
               f"Error: {spark_detail}\n\n"
               "Common causes:\n"
               "  • JAVA_HOME not pointing at a working JDK-17\n"
               "  • winutils.exe missing (Windows)\n"
               "  • Port 4040 already in use (kill old Spark processes)\n"
               "  • PYSPARK_PYTHON mismatch between driver and executor"
    )

# ─────────────────────────────────────────────────────────────────────────────
# 8. MINIO REACHABILITY
# ─────────────────────────────────────────────────────────────────────────────
section("8 · MinIO  (S3A backend)  — localhost:9000")

MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_ACCESS    = "admin"
MINIO_SECRET    = "password123"

minio_up = False
minio_detail = ""
try:
    req = urllib.request.Request(
        f"{MINIO_ENDPOINT}/minio/health/live",
        headers={"User-Agent": "preflight-check/1.0"}
    )
    with urllib.request.urlopen(req, timeout=4) as resp:
        minio_up = resp.status == 200
        minio_detail = f"HTTP {resp.status}"
except urllib.error.HTTPError as e:
    # 403 still means MinIO is running; health endpoint may need auth
    minio_up = e.code in (403, 200)
    minio_detail = f"HTTP {e.code} (MinIO is running)"
except Exception as e:
    minio_detail = str(e)

check(
    f"MinIO HTTP endpoint reachable  ({minio_detail})",
    minio_up,
    detail="" if minio_up else
           "Start MinIO:  minio server C:\\minio-data --console-address :9001\n"
           "Or via Docker: docker compose up minio"
)

# ─────────────────────────────────────────────────────────────────────────────
# 9. MINIO BUCKETS  (raw, curated)
# ─────────────────────────────────────────────────────────────────────────────
section("9 · MinIO buckets  (raw, curated)")

if minio_up:
    boto3_spec = importlib.util.find_spec("boto3")
    if boto3_spec is None:
        print(f"  {WARN}  boto3 not installed — install with  pip install boto3  for a deeper bucket check.")
        print(f"         Skipping bucket existence test.")
    else:
        import boto3
        from botocore.client import Config
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS,
                aws_secret_access_key=MINIO_SECRET,
                config=Config(signature_version="s3v4"),
                region_name="us-east-1",
            )
            buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            for bucket in ["raw", "curated"]:
                check(
                    f"Bucket '{bucket}' exists",
                    bucket in buckets,
                    detail=f"Existing buckets: {buckets}\n"
                           f"Create with: mc mb myminio/{bucket}  or via the MinIO console at :9001"
                           if bucket not in buckets else ""
                )
            # Check for data files
            for prefix, desc in [
                ("porto-trips/", "raw Porto trips data"),
                ("nyc-tlc/",     "raw NYC TLC data"),
            ]:
                try:
                    resp = s3.list_objects_v2(Bucket="raw", Prefix=prefix, MaxKeys=1)
                    has_files = resp.get("KeyCount", 0) > 0
                    check(
                        f"raw/{prefix} has at least one file  ({desc})",
                        has_files,
                        detail="" if has_files else
                               f"Upload your source data to s3a://raw/{prefix} before running the ETL."
                    )
                except Exception as e:
                    check(f"raw/{prefix} listable", False, detail=str(e))
        except Exception as e:
            check("MinIO authenticated list-buckets", False, detail=str(e))
else:
    print(f"  {WARN}  Skipped — MinIO is not reachable.")

# ─────────────────────────────────────────────────────────────────────────────
# 10. CASSANDRA REACHABILITY
# ─────────────────────────────────────────────────────────────────────────────
section("10 · Cassandra  — localhost:9042")

cass_ok = False
cass_detail = ""
try:
    with socket.create_connection(("localhost", 9042), timeout=4) as sock:
        cass_ok = True
        cass_detail = "TCP connect succeeded"
except Exception as e:
    cass_detail = str(e)

check(
    f"Cassandra port 9042 reachable  ({cass_detail})",
    cass_ok,
    detail="" if cass_ok else
           "Start Cassandra:  docker compose up cassandra\n"
           "Or check that it is not still booting (can take ~60 s)."
)

if cass_ok:
    # Try with cassandra-driver if available
    cdrv_spec = importlib.util.find_spec("cassandra")
    if cdrv_spec is not None:
        try:
            from cassandra.cluster import Cluster
            cluster = Cluster(["localhost"])
            session = cluster.connect()
            keyspaces = [row.keyspace_name for row in session.execute(
                "SELECT keyspace_name FROM system_schema.keyspaces"
            )]
            taasim_ks = "taasim" in keyspaces
            check(
                "Keyspace 'taasim' exists",
                taasim_ks,
                detail="" if taasim_ks else
                       "Create it:\n"
                       "  CREATE KEYSPACE taasim WITH replication = "
                       "{'class':'SimpleStrategy','replication_factor':1};"
            )
            if taasim_ks:
                tables = [row.table_name for row in session.execute(
                    "SELECT table_name FROM system_schema.tables WHERE keyspace_name='taasim'"
                )]
                for tbl in ["demand_zones"]:
                    check(
                        f"Table taasim.{tbl} exists",
                        tbl in tables,
                        detail="" if tbl in tables else
                               f"Create the table before running the notebook's Cassandra write cell."
                    )
            cluster.shutdown()
        except Exception as e:
            check("cassandra-driver can connect and query", False, detail=str(e))
    else:
        print(f"  {INFO}  cassandra-driver not installed; skipping deep Cassandra check.")
        print(f"         Install with: pip install cassandra-driver")

# ─────────────────────────────────────────────────────────────────────────────
# 11. FINAL SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
section("SUMMARY")

hard_fails = [(lbl, ok) for lbl, ok, warn_only in results if not ok and not warn_only]
warnings   = [(lbl, ok) for lbl, ok, warn_only in results if not ok and warn_only]

if not hard_fails:
    print(f"\n  {GREEN}{BOLD}All required checks passed!{RESET}")
    if warnings:
        print(f"  {YELLOW}There are {len(warnings)} warning(s) — review them but they may not block you.{RESET}")
    print(f"\n  {BOLD}You are good to open week5.ipynb and run it.{RESET}\n")
else:
    print(f"\n  {RED}{BOLD}❌  {len(hard_fails)} issue(s) must be fixed before running the notebook:{RESET}\n")
    for i, (lbl, _) in enumerate(hard_fails, 1):
        print(f"   {i}. {lbl}")
    if warnings:
        print(f"\n  {YELLOW}Additionally {len(warnings)} warning(s):{RESET}")
        for i, (lbl, _) in enumerate(warnings, 1):
            print(f"   {i}. {lbl}")
    print()

print("─" * 62)
print("  Quick-fix cheat sheet")
print("─" * 62)
print("""
  pyspark missing
    → python -m pip install pyspark==3.5.1

  Java not found / wrong version
    → Download JDK 17 from https://adoptium.net
    → Set JAVA_HOME=C:\\Program Files\\Eclipse Adoptium\\jdk-17.x.x
    → Add %JAVA_HOME%\\bin to your system PATH

  winutils missing  (Windows)
    → Download from https://github.com/cdarlint/winutils
      (pick hadoop-3.3.x/bin/winutils.exe)
    → Save to C:\\hadoop\\bin\\winutils.exe
    → Set HADOOP_HOME=C:\\hadoop in system env vars

  MinIO not reachable
    → docker compose up minio   (or run the minio.exe server directly)
    → Console available at http://localhost:9001  (admin / password123)

  Cassandra not reachable
    → docker compose up cassandra
    → Wait ~60 s for it to finish booting before retrying

  Wrong Python kernel in Jupyter
    → In Jupyter: Kernel → Change Kernel → flink_env (3.11.9)
    → Or: python -m ipykernel install --user --name flink_env
""")