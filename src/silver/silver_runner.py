# Databricks notebook source
# COMMAND ----------
import yaml

# ==========================================================
# Job widgets (20% - orchestration only)
# ==========================================================
def _w(name: str, default: str):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

_w("env", "dev")
_w("tenant_id", "tenant_demo")   # optional; config is source of truth
_w("site_id", "site_demo")       # optional; config is source of truth
_w("config_file", "configs/tenants/tenant_demo/dev.yml")
_w("run_minutes", "5")

# Optional: debug only (do NOT use in normal runs)
_w("event_type_filter", "")  # e.g. "temp_humidity.v1" to process only one event_type for debugging

# Read widgets (safe defaults; YAML will override later)
env = (dbutils.widgets.get("env") or "dev").strip()
tenant_id_widget = (dbutils.widgets.get("tenant_id") or "").strip()
site_id_widget = (dbutils.widgets.get("site_id") or "").strip()
config_file = (dbutils.widgets.get("config_file") or "").strip()
event_type_filter = (dbutils.widgets.get("event_type_filter") or "").strip()

# Temporary default until YAML loads (prevents NameError)
#run_minutes = float((dbutils.widgets.get("run_minutes") or "5").strip() or "5")

def _parse_run_minutes(raw, default: int = 5) -> int:
    s = "" if raw is None else str(raw).strip()
    if s == "":
        return default
    try:
        return max(1, int(float(s)))
    except Exception:
        return default

run_minutes = _parse_run_minutes(dbutils.widgets.get("run_minutes"), default=5)

print(f"[runner params] env={env} tenant_id(widget)={tenant_id_widget} site_id(widget)={site_id_widget} run_minutes(widget/default)={run_minutes}")
print(f"[runner params] config_file={config_file} event_type_filter={event_type_filter}")

# ==========================================================
# Load tenant config (20%)
# bundle-safe: resolve local file path on the driver under /Workspace
# ==========================================================
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_ws_path = ctx.notebookPath().get()

bundle_ws_root = nb_ws_path.split("/src/")[0]     # runner lives under .../src/...
bundle_local_root = "/Workspace" + bundle_ws_root
config_local_path = f"{bundle_local_root}/{config_file}"

print(f"[runner] notebookPath={nb_ws_path}")
print(f"[runner] bundle_ws_root={bundle_ws_root}")
print(f"[runner] config_local_path={config_local_path}")

with open(config_local_path, "r") as f:
    cfg = yaml.safe_load(f) or {}

tenant_cfg  = cfg.get("tenant", {}) or {}
ing_cfg     = cfg.get("ingestion", {}) or {}
storage_cfg = cfg.get("storage", {}) or {}
events_cfg  = cfg.get("events", {}) or {}
runtime_cfg = cfg.get("runtime", {}) or {}

# Now that YAML is loaded, resolve run_minutes correctly (YAML default, widget override)
# Resolve run_minutes (YAML default, widget override; widget wins)
yaml_minutes = runtime_cfg.get("run_minutes", 5)
run_minutes = _parse_run_minutes(dbutils.widgets.get("run_minutes"), default=_parse_run_minutes(yaml_minutes, default=5))
print(f"[runner params] final_run_minutes={run_minutes}")

tenant_id = tenant_cfg.get("tenant_id") or tenant_id_widget
site_id   = tenant_cfg.get("site_id_default") or site_id_widget

allowed_event_types = events_cfg.get("allowed_event_types") or []
if not allowed_event_types:
    raise Exception("Config error: events.allowed_event_types is empty")

# Where contracts live (20% config; default inside bundle)
# We'll store per-event contracts in: configs/contracts/<event_type>.yml
#contracts_dir = events_cfg.get("contracts_dir") or "configs/contracts"

# Storage base path (ONE place to change per client/workspace)
base_path = storage_cfg.get("base_path")
if not base_path:
    raise Exception("Config error: storage.base_path is missing")

# Final authority: YAML can still override if widget not set (already handled),
# but keep this line if you want YAML to always win even when widget is set:
# run_minutes = float(runtime_cfg.get("run_minutes", run_minutes))

print(f"[runner config] tenant_id={tenant_id} site_id={site_id}")
print(f"[runner config] base_path={base_path}")
print(f"[runner config] allowed_event_types={allowed_event_types}")
#print(f"[runner config] contracts_dir={contracts_dir} run_minutes={run_minutes}")

# ==========================================================
# Hand-off to Silver Collector (80%)
# ==========================================================
# COMMAND ----------

%run "/Workspace/Users/info@justaboutdata.com/.bundle/streaming_platform_engine/dev/files/src/silver_collector"


# COMMAND ----------

queries = run_silver_collector(
    env=env,
    tenant_id=tenant_id,
    site_id=site_id,
    base_path=base_path,
    allowed_event_types=allowed_event_types,
    schemas_base_path=f"{bundle_local_root}/schemas/event_types",
    run_minutes=run_minutes,
    event_type_filter=event_type_filter,
)

print("[runner] silver collector started queries:", list(queries.keys()))