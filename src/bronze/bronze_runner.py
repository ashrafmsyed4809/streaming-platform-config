# Databricks notebook source
# COMMAND ----------
import yaml
import os
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ==========================================================
# Job widgets (20% - runtime/orchestration only)
# ==========================================================
def _w(name: str, default: str):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

_w("env", "dev")
_w("tenant_id", "tenant_demo")
_w("site_id", "site_demo")
_w("config_file", "configs/tenants/tenant_demo/dev.yml")
_w("run_minutes", "5")

# NEW: source_id lets you analyze per sensor/source stream
_w("source_id", "default_source")

# Optional overrides (nice for testing)
_w("source_override", "")        # "eventhub" or "files" or empty
_w("landing_path_override", "")  # only used when source=files

# Read widgets
env = (dbutils.widgets.get("env") or "dev").strip()
tenant_id_widget = (dbutils.widgets.get("tenant_id") or "").strip()
site_id_widget = (dbutils.widgets.get("site_id") or "").strip()
config_file = (dbutils.widgets.get("config_file") or "").strip()
#run_minutes = int(dbutils.widgets.get("run_minutes") or "5")
def _parse_run_minutes(raw, default: int = 5) -> int:
    s = "" if raw is None else str(raw).strip()
    if s == "":
        return default
    try:
        return max(1, int(float(s)))
    except Exception:
        return default

run_minutes = _parse_run_minutes(dbutils.widgets.get("run_minutes"), default=5)

source_id = (dbutils.widgets.get("source_id") or "default_source").strip()

source_override = (dbutils.widgets.get("source_override") or "").strip().lower()
landing_path_override = (dbutils.widgets.get("landing_path_override") or "").strip()

print(f"[runner params] env={env} tenant_id(widget)={tenant_id_widget} site_id(widget)={site_id_widget} run_minutes={run_minutes}")
print(f"[runner params] config_file={config_file}")
print(f"[runner params] source_id(widget)={source_id}")
print(f"[runner params] source_override={source_override} landing_path_override={landing_path_override}")

# ==========================================================
# Load tenant config (20%)
# ==========================================================
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_ws_path = ctx.notebookPath().get()

# bundle-safe: resolve local file path on the driver under /Workspace
bundle_ws_root = nb_ws_path.split("/src/")[0]   # assuming runner lives under .../src/... in bundle
bundle_local_root = "/Workspace" + bundle_ws_root
config_local_path = f"{bundle_local_root}/{config_file}"

print(f"[runner] notebookPath={nb_ws_path}")
print(f"[runner] bundle_ws_root={bundle_ws_root}")
print(f"[runner] config_local_path={config_local_path}")

with open(config_local_path, "r") as f:
    cfg = yaml.safe_load(f) or {}

tenant_cfg = cfg.get("tenant", {}) or {}
ing_cfg = cfg.get("ingestion", {}) or {}
events_cfg = cfg.get("events", {}) or {}
runtime_cfg = cfg.get("runtime", {}) or {}

# ================= DEBUG START =================
print("\n[DEBUG] config_local_path =", config_local_path)
print("[DEBUG] tenant_cfg keys =", list(tenant_cfg.keys()))
print("[DEBUG] ingestion keys =", list(ing_cfg.keys()))
print("[DEBUG] events keys =", list(events_cfg.keys()))
print("[DEBUG] device_registry preview =",
      list((events_cfg.get("device_registry") or {}).items())[:10])
print("=============================================\n")
# ================= DEBUG END =================

# Single source of truth from config
tenant_id = tenant_cfg.get("tenant_id") or tenant_id_widget
site_id = tenant_cfg.get("site_id_default") or site_id_widget

allowed_event_types = events_cfg.get("allowed_event_types") or []
if not allowed_event_types:
    raise Exception("Config error: events.allowed_event_types is empty")

device_registry = events_cfg.get("device_registry") or {}

# Ingestion source for this tenant (can be overridden by widget)
source = (ing_cfg.get("source") or "eventhub").strip().lower()
if source_override:
    source = source_override

landing_path = ing_cfg.get("landing_path") or "/Volumes/platform/dev/streaming_platform/env=dev/landing"
if landing_path_override:
    landing_path = landing_path_override

# YAML can override source_id
source_id = (ing_cfg.get("source_id") or source_id).strip()

# Optional: allow config runtime override
#run_minutes = int(runtime_cfg.get("run_minutes", run_minutes))
# Resolve run_minutes (YAML default, widget override; widget wins)
yaml_minutes = runtime_cfg.get("run_minutes", 5)
run_minutes = _parse_run_minutes(dbutils.widgets.get("run_minutes"), default=_parse_run_minutes(yaml_minutes, default=5))
print(f"[runner params] final_run_minutes={run_minutes}")

print(f"[runner config] tenant_id={tenant_id} site_id={site_id}")
print(f"[runner config] source={source} landing_path={landing_path}")
print(f"[runner config] source_id={source_id}")
print(f"[runner config] allowed_event_types={allowed_event_types}")
print(f"[runner config] device_registry_keys={len(device_registry)} run_minutes={run_minutes}")

# ==========================================================
# Event Hub setup (ENTERPRISE SAFE)
# ==========================================================
EH_NAME = ing_cfg.get("eventhub_name")
if not EH_NAME:
    raise Exception("Config error: ingestion.eventhub_name is missing")

# Pull secret directly from Databricks secret scope backed by Azure Key Vault
# This avoids any YAML interpolation issues in jobs/platform_job.yml
EH_NAMESPACE_CONN_STR = dbutils.secrets.get("streaming-kv", "eventhub-namespace-conn-str")

# Safety/debug: ensure we didn't get a template string
if ("{{" in EH_NAMESPACE_CONN_STR) or ("}}" in EH_NAMESPACE_CONN_STR):
    raise Exception("Secret resolution failed: got a template string instead of a real connection string.")

if not EH_NAMESPACE_CONN_STR.strip().startswith("Endpoint=sb://"):
    raise Exception("Secret value does not look like an Event Hubs namespace connection string (expected 'Endpoint=sb://...').")

conn_str = EH_NAMESPACE_CONN_STR.rstrip(";") + f";EntityPath={EH_NAME}"

ehConf = {
    "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str)
}

print("[runner] Event Hub config ready (namespace secret loaded, EntityPath appended).")


#========================================================
# Hand-off to Bronze Collector (80%)
# ==========================================================

# COMMAND ----------
%run "/Users/info@justaboutdata.com/streaming_platform_engine/bronze/bronze_collector"

# COMMAND ----------
base = f"/Volumes/platform/dev/streaming_platform/env={env}"

queries = run_bronze_collector(
    env=env,
    tenant_id=tenant_id,
    site_id=site_id,
    source=source,
    source_id=source_id,
    landing_path=landing_path,
    allowed_event_types=allowed_event_types,
    device_registry=device_registry,
    run_minutes=run_minutes,
    ehConf=ehConf,
    bronze_envelope_path=f"{base}/bronze_envelope_v2",
    dlq_envelope_path=f"{base}/dlq_envelope_v2",
    bronze_ckpt=f"{base}/checkpoints/bronze/envelope_v2",
    dlq_ckpt=f"{base}/checkpoints/dlq/envelope_v2",
)

print("[runner] collector started queries:", list(queries.keys()))