#!/usr/bin/env bash
set -euo pipefail
set -a
source .env
set +a

PROJECT="${GCP_PROJECT_ID:?GCP_PROJECT_ID is not set in .env}"
REGION="${GCP_REGION:?GCP_REGION is not set in .env}"
CLUSTER_NAME="${GCP_CLUSTER_NAME:?GCP_CLUSTER_NAME is not set in .env}"
BUCKET_NAME="${GCP_BUCKET_NAME:?GCP_BUCKET_NAME is not set in .env}"

LOCAL_SPARK_FILE="assets/spark_job.py"
LOCAL_CRIME_FILE="${LOCAL_CRIME_FILE:-}"
LOCAL_ZILLOW_FILE="${LOCAL_ZILLOW_FILE:-}"
LOCAL_BOUNDARY_FILE="${LOCAL_BOUNDARY_FILE:-}"

# Canonical GCS object names used by downstream steps.
CRIME_OBJECT_NAME="${CRIME_OBJECT_NAME:-chicago_crime.csv}"
ZILLOW_OBJECT_NAME="${ZILLOW_OBJECT_NAME:-chicago_zillow_home_values.csv}"
BOUNDARY_OBJECT_NAME="${BOUNDARY_OBJECT_NAME:-chicago_zip_boundaries.csv}"
LOCAL_POPULATION_FILE="${LOCAL_POPULATION_FILE:-historical_zip_population_10yrs.csv}"
SNOWFLAKE_STAGE_NAME="${SNOWFLAKE_STAGE:?SNOWFLAKE_STAGE is not set in .env}"
FORCE_DATASET_UPLOAD="${FORCE_DATASET_UPLOAD:-0}"

echo "===================================="
echo "PROJECT:      ${PROJECT}"
echo "REGION:       ${REGION}"
echo "CLUSTER_NAME: ${CLUSTER_NAME}"
echo "BUCKET_NAME:  ${BUCKET_NAME}"
echo "STAGE:        ${SNOWFLAKE_STAGE_NAME}"
echo "SPARK_FILE:   ${LOCAL_SPARK_FILE}"
echo "===================================="

find_latest_file() {
  local pattern
  for pattern in "$@"; do
    local match
    match=$(ls -1t ${pattern} 2>/dev/null | head -n 1 || true)
    if [[ -n "${match}" ]]; then
      echo "${match}"
      return 0
    fi
  done
  return 1
}

detect_csv_by_schema() {
  local dataset_type="$1"

  python3 - "$dataset_type" <<'PY'
import csv
import glob
import sys

dataset_type = sys.argv[1]
paths = sorted(glob.glob("*.csv"))

def headers_for(path):
  try:
    with open(path, newline="", encoding="utf-8-sig") as fh:
      reader = csv.DictReader(fh)
      return reader.fieldnames or []
  except Exception:
    return []

def score_crime(headers):
  need = {"ID", "Date", "Primary Type", "Latitude", "Longitude"}
  return len(need & set(headers))

def score_zillow(headers):
  need = {"RegionName", "RegionType", "State", "City"}
  return len(need & set(headers))

def score_boundary(headers):
  lower = [h.lower() for h in headers]
  has_zip = "zip" in headers
  has_geom = any(("geom" in h) or ("shape" in h) for h in lower)
  return (1 if has_zip else 0) + (1 if has_geom else 0)

scorer = {
  "crime": score_crime,
  "zillow": score_zillow,
  "boundary": score_boundary,
}.get(dataset_type)

if scorer is None:
  sys.exit(0)

best_path = None
best_score = -1

for p in paths:
  if p == "historical_zip_population_10yrs.csv":
    continue
  headers = headers_for(p)
  if not headers:
    continue
  s = scorer(headers)
  if s > best_score:
    best_score = s
    best_path = p

threshold = {"crime": 5, "zillow": 4, "boundary": 2}[dataset_type]
if best_path and best_score >= threshold:
  print(best_path)
PY
}

check_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "ERROR: Required command '${command_name}' is not installed or not on PATH."
    exit 1
  fi
}

check_local_file() {
  local file_path="$1"
  local label="$2"
  if [[ ! -f "${file_path}" ]]; then
    echo "ERROR: Missing ${label} at ${file_path}"
    exit 1
  fi
}

ensure_python_dependency() {
  local module_name="$1"
  local package_name="$2"
  if ! python3 -c "import ${module_name}" >/dev/null 2>&1; then
    echo "Installing missing Python package: ${package_name}"
    python3 -m pip install "${package_name}"
  fi
}

validate_zillow_zip_level() {
  local zillow_path="$1"

  python3 - "$zillow_path" <<'PY'
import csv
import sys

path = sys.argv[1]

try:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            raise ValueError("missing header row")

        required_cols = {"RegionName", "RegionType", "State", "City"}
        missing = sorted(required_cols - set(reader.fieldnames))
        if missing:
            raise ValueError(f"missing required columns: {', '.join(missing)}")

        checked_rows = 0
        saw_zip_regiontype = False
        for row in reader:
            checked_rows += 1
            region_type = (row.get("RegionType") or "").strip().lower()
            if region_type == "zip":
                saw_zip_regiontype = True
                break
            if checked_rows >= 1000:
                break

        if not saw_zip_regiontype:
            raise ValueError(
                "RegionType is not ZIP in sampled rows (likely Metro/City/County dataset)"
            )

except Exception as exc:
    print("ERROR: Zillow dataset validation failed.")
    print(f"File: {path}")
    print(f"Reason: {exc}")
    print(
        "Please download the ZIP-level Zillow file (e.g. Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv) "
        "and set LOCAL_ZILLOW_FILE in .env if needed."
    )
    sys.exit(1)

print(f"Zillow ZIP-level validation passed: {path}")
PY
}

validate_crime_file() {
  local crime_path="$1"

  python3 - "$crime_path" <<'PY'
import csv
import sys

path = sys.argv[1]

try:
  with open(path, newline="", encoding="utf-8-sig") as fh:
    reader = csv.DictReader(fh)
    headers = set(reader.fieldnames or [])
    required = {"ID", "Date", "Primary Type", "Latitude", "Longitude"}
    missing = sorted(required - headers)
    if missing:
      raise ValueError(f"missing required columns: {', '.join(missing)}")
except Exception as exc:
  print("ERROR: Crime dataset validation failed.")
  print(f"File: {path}")
  print(f"Reason: {exc}")
  print("Please use the Chicago crimes CSV download and set LOCAL_CRIME_FILE in .env if needed.")
  sys.exit(1)

print(f"Crime dataset validation passed: {path}")
PY
}

validate_boundary_file() {
  local boundary_path="$1"

  python3 - "$boundary_path" <<'PY'
import csv
import sys

path = sys.argv[1]

try:
  with open(path, newline="", encoding="utf-8-sig") as fh:
    reader = csv.DictReader(fh)
    headers = reader.fieldnames or []
    header_set = set(headers)
    lower = [h.lower() for h in headers]

    if "ZIP" not in header_set:
      raise ValueError("missing required column: ZIP")
    if not any(("geom" in h) or ("shape" in h) for h in lower):
      raise ValueError("missing geometry column (expected name containing 'geom' or 'shape')")
except Exception as exc:
  print("ERROR: Boundary dataset validation failed.")
  print(f"File: {path}")
  print(f"Reason: {exc}")
  print("Please use the Chicago ZIP boundary CSV download and set LOCAL_BOUNDARY_FILE in .env if needed.")
  sys.exit(1)

print(f"Boundary dataset validation passed: {path}")
PY
}

ensure_population_csv() {
  if [[ -f "${LOCAL_POPULATION_FILE}" ]]; then
    echo "Population CSV found at ${LOCAL_POPULATION_FILE}"
    return
  fi

  if [[ "${LOCAL_POPULATION_FILE}" != "historical_zip_population_10yrs.csv" ]]; then
    echo "ERROR: Population CSV not found at ${LOCAL_POPULATION_FILE}"
    echo "Please create it first or point LOCAL_POPULATION_FILE to the generated file."
    exit 1
  fi

  echo "Population CSV missing. Generating it with population_data.py..."
  python3 population_data.py
  check_local_file "${LOCAL_POPULATION_FILE}" "population CSV"
}

check_gcs_object() {
  local object_name="$1"
  if gcloud storage ls "gs://${BUCKET_NAME}/${object_name}" >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

upload_if_missing() {
  local local_path="$1"
  local object_name="$2"
  local label="$3"

  check_local_file "${local_path}" "${label}"

  if check_gcs_object "${object_name}"; then
    echo "${label} already exists in GCS: gs://${BUCKET_NAME}/${object_name}"
  else
    echo "Uploading ${label} to gs://${BUCKET_NAME}/${object_name}"
    gcloud storage cp "${local_path}" "gs://${BUCKET_NAME}/${object_name}"
  fi
}

upload_dataset() {
  local local_path="$1"
  local object_name="$2"
  local label="$3"

  check_local_file "${local_path}" "${label}"

  if check_gcs_object "${object_name}" && [[ "${FORCE_DATASET_UPLOAD}" != "1" ]]; then
    echo "${label} already exists in GCS (skipping): gs://${BUCKET_NAME}/${object_name}"
    return
  fi

  if check_gcs_object "${object_name}"; then
    echo "Re-uploading ${label} because FORCE_DATASET_UPLOAD=1: ${local_path} -> gs://${BUCKET_NAME}/${object_name}"
  else
    echo "Uploading ${label}: ${local_path} -> gs://${BUCKET_NAME}/${object_name}"
  fi

  gcloud storage cp "${local_path}" "gs://${BUCKET_NAME}/${object_name}"
}

verify_snowflake_stage() {
  python3 - <<'PY'
import os
import sys

try:
    import snowflake.connector
except ModuleNotFoundError:
    print("ERROR: snowflake-connector-python is required for Snowflake preflight checks.")
    sys.exit(1)

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema=os.environ["SNOWFLAKE_SCHEMA"],
    role=os.environ.get("SNOWFLAKE_ROLE"),
)

stage_name = os.environ["SNOWFLAKE_STAGE"]

try:
    with conn.cursor() as cur:
        cur.execute(f"LIST @{stage_name}")
        print(f"Snowflake stage check passed: @{stage_name}")
except Exception as exc:
    print("ERROR: Snowflake stage check failed.")
    print(
        "Create and authorize the stage first using assets/setup_snowflake_stage.sql, "
        "then make sure SNOWFLAKE_STAGE in .env matches the created stage name."
    )
    print(f"Original error: {exc}")
    sys.exit(1)
finally:
    conn.close()
PY
}

# set project
check_command gcloud
check_command python3
ensure_python_dependency "snowflake.connector" "snowflake-connector-python"

gcloud config set project "${PROJECT}"

if [[ -z "${LOCAL_CRIME_FILE}" ]]; then
  LOCAL_CRIME_FILE=$(find_latest_file \
    "chicago_crime.csv" \
    "Crimes_-_2001_to_Present*.csv" \
    "Crimes-2001-to-Present*.csv" || true)
fi

if [[ -z "${LOCAL_CRIME_FILE}" ]]; then
  LOCAL_CRIME_FILE=$(detect_csv_by_schema "crime" || true)
fi

if [[ -z "${LOCAL_ZILLOW_FILE}" ]]; then
  LOCAL_ZILLOW_FILE=$(find_latest_file \
    "chicago_zillow_home_values.csv" \
    "Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month*.csv" \
    "Metro_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month*.csv" || true)
fi

if [[ -z "${LOCAL_ZILLOW_FILE}" ]]; then
  LOCAL_ZILLOW_FILE=$(detect_csv_by_schema "zillow" || true)
fi

if [[ -z "${LOCAL_BOUNDARY_FILE}" ]]; then
  LOCAL_BOUNDARY_FILE=$(find_latest_file \
    "chicago_zip_boundaries.csv" \
    "Boundaries_-_ZIP_Codes_*.csv" \
    "Boundaries_-_ZIP_Codes*.csv" \
    "Boundaries-ZIP-Codes*.csv" || true)
fi

if [[ -z "${LOCAL_BOUNDARY_FILE}" ]]; then
  LOCAL_BOUNDARY_FILE=$(detect_csv_by_schema "boundary" || true)
fi

# preflight checks before any compute starts
check_local_file "${LOCAL_SPARK_FILE}" "Spark job"
check_local_file "${LOCAL_CRIME_FILE}" "crime CSV"
validate_crime_file "${LOCAL_CRIME_FILE}"
check_local_file "${LOCAL_ZILLOW_FILE}" "Zillow CSV"
validate_zillow_zip_level "${LOCAL_ZILLOW_FILE}"
check_local_file "${LOCAL_BOUNDARY_FILE}" "boundary CSV"
validate_boundary_file "${LOCAL_BOUNDARY_FILE}"
ensure_population_csv

echo "Resolved local files:"
echo "  crime:    ${LOCAL_CRIME_FILE}"
echo "  zillow:   ${LOCAL_ZILLOW_FILE}"
echo "  boundary: ${LOCAL_BOUNDARY_FILE}"
echo "Canonical GCS object names:"
echo "  crime:    ${CRIME_OBJECT_NAME}"
echo "  zillow:   ${ZILLOW_OBJECT_NAME}"
echo "  boundary: ${BOUNDARY_OBJECT_NAME}"

echo "Verifying access to GCS bucket..."
if ! gcloud storage ls "gs://${BUCKET_NAME}" >/dev/null 2>&1; then
  echo "ERROR: Cannot access gs://${BUCKET_NAME}. Check that the bucket exists and your gcloud auth has access."
  exit 1
fi

echo "Uploading required datasets to GCS if missing..."
upload_dataset "${LOCAL_CRIME_FILE}" "${CRIME_OBJECT_NAME}" "crime CSV"
upload_dataset "${LOCAL_ZILLOW_FILE}" "${ZILLOW_OBJECT_NAME}" "Zillow CSV"
upload_dataset "${LOCAL_BOUNDARY_FILE}" "${BOUNDARY_OBJECT_NAME}" "boundary CSV"
upload_if_missing "${LOCAL_POPULATION_FILE}" "historical_zip_population_10yrs.csv" "population CSV"

echo "Verifying Snowflake stage access before starting the pipeline..."
verify_snowflake_stage

# create cluster if it does not already exist
if ! gcloud dataproc clusters describe "${CLUSTER_NAME}" --region="${REGION}" >/dev/null 2>&1; then
  echo "Creating Dataproc cluster ${CLUSTER_NAME}..."

  gcloud dataproc clusters create "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --single-node \
    --master-machine-type=n2-highmem-4 \
    --image-version=2.1-debian11 \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-west1/python/pip-install.sh \
    --metadata="PIP_PACKAGES=apache-sedona==1.5.1 snowflake-connector-python[pandas]" \
    --properties="^#^spark:spark.jars.packages=org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1#spark:spark.jars.repositories=https://artifacts.unidata.ucar.edu/repository/unidata-all/#spark:spark.sql.extensions=org.apache.sedona.sql.SedonaSqlExtensions#spark:spark.serializer=org.apache.spark.serializer.KryoSerializer#spark:spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator"
else
  echo "Cluster ${CLUSTER_NAME} already exists."
fi

echo "Submitting PySpark job..."

gcloud dataproc jobs submit pyspark "${LOCAL_SPARK_FILE}" \
  --cluster="${CLUSTER_NAME}" \
  --region="${REGION}" \
  --properties="^#^spark.jars.packages=org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.5.1,org.datasyslab:geotools-wrapper:1.5.1-28.2#spark.sql.adaptive.enabled=true#spark.sql.shuffle.partitions=128#spark.executor.instances=1" \
  -- "${BUCKET_NAME}" "${CRIME_OBJECT_NAME}" "${ZILLOW_OBJECT_NAME}" "${BOUNDARY_OBJECT_NAME}"
 
echo "######## Job finished."

echo "Loading curated GCS data into Snowflake"
python3 assets/snowflake_load.py
echo "######## Process complete."
