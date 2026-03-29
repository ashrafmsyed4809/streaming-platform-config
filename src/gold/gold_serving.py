# Databricks notebook source
# COMMAND ----------

# ====== Job widgets (minimal) ======
def _w(name: str, default: str):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

_w("env", "dev")
_w("tenant_id", "tenant_demo")
_w("site_id", "site_demo")
_w("event_type", "temp_humidity.v1")
_w("config_file", "configs/tenants/tenant_demo/dev.yml")
_w("run_minutes", "5")
_w("event_type_override", "")

# Read widgets
env = dbutils.widgets.get("env").strip() or "dev"
tenant_id = dbutils.widgets.get("tenant_id").strip() or "tenant_demo"
site_id = dbutils.widgets.get("site_id").strip() or "site_demo"
event_type = dbutils.widgets.get("event_type").strip() or "temp_humidity.v1"
config_file = dbutils.widgets.get("config_file").strip() or "configs/tenants/tenant_demo/dev.yml"
event_type_override = dbutils.widgets.get("event_type_override").strip()
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

print(f"[runner params] env={env} tenant_id={tenant_id} site_id={site_id} event_type={event_type} run_minutes={run_minutes}")
print(f"[runner params] config_file={config_file}")

# COMMAND ----------

# ====== Load tenant config (80/20) ======
import os
import yaml

def find_bundle_root(start_dir: str) -> str:
    cur = os.path.abspath(start_dir)
    for _ in range(10):
        if os.path.isdir(os.path.join(cur, "configs")):
            return cur
        cur = os.path.dirname(cur)
    raise Exception(f"Could not find bundle root containing /configs from start_dir={start_dir}")

bundle_root = find_bundle_root(os.getcwd())
cfg_path = os.path.join(bundle_root, config_file)

print("[runner] cwd =", os.getcwd())
print("[runner] bundle_root =", bundle_root)
print("[runner] cfg_path =", cfg_path)

with open(cfg_path, "r") as f:
    cfg = yaml.safe_load(f) or {}

# Override from config (single source of truth)
tenant_id = cfg["tenant"]["tenant_id"]
site_id = cfg["tenant"].get("site_id_default", site_id)

allowed_event_types = cfg["events"].get("allowed_event_types", [])
if not allowed_event_types:
    raise Exception("Config error: events.allowed_event_types is empty")

# Project 01: one event_type per run (use first one)
#event_type = allowed_event_types[0]
event_type = event_type_override if event_type_override else allowed_event_types[0]
if event_type not in allowed_event_types:
    raise Exception(f"Invalid event_type '{event_type}'. Allowed: {allowed_event_types}")
# runtime override (optional)
#run_minutes = int(cfg.get("runtime", {}).get("run_minutes", run_minutes))
# Resolve run_minutes (YAML default, widget override; widget wins)
yaml_minutes = cfg.get("runtime", {}).get("run_minutes", 5)
run_minutes = _parse_run_minutes(dbutils.widgets.get("run_minutes"), default=_parse_run_minutes(yaml_minutes, default=5))
print(f"[runner params] final_run_minutes={run_minutes}")

print(f"[runner config] tenant_id={tenant_id} site_id={site_id} event_type={event_type} run_minutes={run_minutes}")
print(f"[runner config] allowed_event_types={allowed_event_types}")
# =====================================

# COMMAND ----------

%run "/Workspace/Users/info@justaboutdata.com/.bundle/streaming_platform_engine/dev/files/gold_collector_batch"
