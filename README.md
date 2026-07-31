# Chicago Crime and Housing Pipeline

This project builds a Chicago ZIP-level analytics dataset by combining:

- Chicago crime data
- Chicago ZIP boundary data
- Zillow home value data
- ZIP-code population data from the US Census API

The pipeline uses:

- Google Cloud Storage for raw and intermediate files
- Google Dataproc + PySpark + Sedona for the spatial join
- Snowflake for loading, joining, and creating analytics views

This guide is written for a first-time user. Follow the steps in order.

## What you need before starting

- A Google Cloud project you can use
- A Google Cloud Storage bucket you own or can write to
- A Snowflake account where you can run SQL as `ACCOUNTADMIN` for the initial setup
- Local access to `gcloud`, already authenticated against the correct project
- Local Python 3 with `pip`

Before you start, make sure these one-time GCP prerequisites are already done:

- billing is enabled on your GCP project
- the `Dataproc API`, `Compute Engine API`, and `Cloud Storage API` are enabled
- `gcloud auth login` has been run for your user
- `gcloud config set project YOUR_PROJECT_ID` works successfully

## Files this pipeline expects

The pipeline ultimately needs these 4 CSV files:

| Dataset | Typical filename | Source |
|---|---|---|
| Crime | `Crimes_-_2001_to_Present_20260302.csv` | Chicago Data Portal |
| ZIP boundaries | `Boundaries_-_ZIP_Codes_20260302.csv` | Chicago Data Portal |
| Zillow ZIP home values | `Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv` | Zillow Research |
| ZIP population | `historical_zip_population_10yrs.csv` | Generated locally by script |

The first three must be downloaded by the user.

The fourth can be generated locally by running:

```bash
python3 population_data.py
```

`population_data.py` is the stable entrypoint used by the pipeline. It calls the repo's fetch script for you.

If `historical_zip_population_10yrs.csv` is missing, `pipeline.sh` will also generate it automatically.

## High-level setup order

Use this exact order:

1. Create or choose a GCS bucket
2. Upload or prepare the local CSV files
3. Create the Snowflake storage integration and external stage for that bucket
4. Grant the Snowflake-generated GCP service account access to the bucket
5. Fill in `.env`
6. Run `bash pipeline.sh`

## Step 1: Clone the repository

```bash
git clone https://github.com/Prof-Rosario-UCLA/team14.git
cd team14
```

## Step 2: Create a GCS bucket

If you already have a bucket, you can reuse it.

If you need a new one, create it yourself in GCP or with `gcloud`. Example:

```bash
gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=us-west1
```

Notes:

- The bucket name must be globally unique.
- The current pipeline does not create a bucket automatically.
- The pipeline will only check that the bucket exists and that you can access it.

## Step 3: Prepare the datasets locally

Download these 3 files and place them somewhere on your machine:

- Crime data: [Chicago Crimes 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/about_data)
- ZIP boundary data: [Chicago ZIP Boundaries](https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-ZIP-Codes/unjd-c2ca/about_data)
- Zillow home value data: [Zillow Research Data](https://www.zillow.com/research/data/)

Download the files so they match these expected datasets:

- `Crimes_-_2001_to_Present_20260302.csv`
- `Boundaries_-_ZIP_Codes_20260302.csv`
- `Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv`

For Zillow, make sure you choose the correct dataset in the dropdowns:

- `Data Type`: `ZHVI All Homes (SFR, Condo/Co-op) Time Series, Smoothed, Seasonally Adjusted($)`
- `Geography`: `ZIP Code`

Generate the population file:

```bash
python3 population_data.py
```

After this step, you should have:

- `historical_zip_population_10yrs.csv`

## Step 4: Configure `.env`

Copy the template:

```bash
cp .env.example .env
```

Then edit `.env`.

Required GCP variables:

```bash
GCP_PROJECT_ID="your-gcp-project-id"
GCP_REGION="us-west1"
GCP_CLUSTER_NAME="my-dataproc-cluster"
GCP_BUCKET_NAME="your-existing-bucket-name"
GCP_BUCKET_URL="gcs://your-existing-bucket-name/"
```

Optional local dataset paths:

Set these only if the CSV files are not in the repo root.

```bash
LOCAL_CRIME_FILE="/absolute/path/to/Crimes_-_2001_to_Present_20260302.csv"
LOCAL_ZILLOW_FILE="/absolute/path/to/Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv"
LOCAL_BOUNDARY_FILE="/absolute/path/to/Boundaries_-_ZIP_Codes_20260302.csv"
LOCAL_POPULATION_FILE="/absolute/path/to/historical_zip_population_10yrs.csv"
```

Required Snowflake variables:

```bash
SNOWFLAKE_USER="your-snowflake-username"
SNOWFLAKE_PASSWORD="your-snowflake-password"
SNOWFLAKE_ACCOUNT="your-account-id"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_DATABASE="CHICAGO_HOUSING_PROJECT"
SNOWFLAKE_SCHEMA="PUBLIC"
SNOWFLAKE_ROLE="SYSADMIN"
SNOWFLAKE_STAGE="gcp_chicago_stage"
SNOWFLAKE_INTEGRATION="gcs_crime_integration"
```

Important:

- `SNOWFLAKE_STAGE` must exactly match the Snowflake stage you create in Step 5.
- `SNOWFLAKE_INTEGRATION` should match the integration name used in the setup SQL.
- `GCP_BUCKET_NAME` must match the bucket used by that Snowflake stage.
- `GCP_BUCKET_URL` should be exactly `gcs://YOUR_BUCKET_NAME/`
- `SNOWFLAKE_ROLE` should match the runtime role that will execute `bash pipeline.sh` later, usually `SYSADMIN`

## Step 5: Create the Snowflake storage integration and stage

Open Snowflake Snowsight and sign in with a role that can create integrations, typically `ACCOUNTADMIN`.

Open [assets/setup_snowflake_stage.sql].

Before running it:

1. Update the configuration block at the top to match your `.env` values.
2. In particular, set `bucket_url`, `integration_name`, `database_name`, `schema_name`, `stage_name`, and `pipeline_role`.
3. Make sure `pipeline_role` matches `SNOWFLAKE_ROLE` from `.env`, or the pipeline may connect successfully but fail stage access later.

The script does these things:

1. Creates a `STORAGE INTEGRATION`
2. Shows the Snowflake-managed GCP service account
3. Creates the database and schema if needed
4. Creates the external stage
5. Grants access to the runtime role

Important behavior:

- The script uses `CREATE ... IF NOT EXISTS`
- If you already created the integration before with the wrong bucket, rerunning the script will not update the allowed locations

If you previously created the integration with the wrong bucket, fix it with:

```sql
ALTER STORAGE INTEGRATION gcs_crime_integration
SET STORAGE_ALLOWED_LOCATIONS = ('gcs://YOUR_BUCKET_NAME/');
```

If you want to recreate the stage cleanly, use:

```sql
CREATE OR REPLACE STAGE gcp_chicago_stage
  URL = 'gcs://YOUR_BUCKET_NAME/'
  STORAGE_INTEGRATION = gcs_crime_integration;
```

## Step 6: Grant the Snowflake service account access to the bucket in GCP

In Snowflake, run:

```sql
DESC INTEGRATION gcs_crime_integration;
```

Find the value of:

- `STORAGE_GCP_SERVICE_ACCOUNT`

That service account must be granted access to your bucket in Google Cloud.

Grant at least this role on the bucket:

- `Storage Object Viewer`

Example:

```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET_NAME \
  --member="serviceAccount:SNOWFLAKE_SERVICE_ACCOUNT" \
  --role="roles/storage.objectViewer"
```

Replace:

- `YOUR_BUCKET_NAME` with your real bucket
- `SNOWFLAKE_SERVICE_ACCOUNT` with the value returned by `DESC INTEGRATION`

Wait about 1 minute after granting the role.

## Step 7: Verify the Snowflake stage manually

Before running the full pipeline, test the stage in Snowflake:

```sql
LIST @gcp_chicago_stage;
```

If this succeeds, Snowflake can read your bucket.

If you get:

- `Location ... is not allowed by integration`

Then your integration allowed locations do not include the bucket.

If you get:

- `does not have storage.objects.list access`

Then the Snowflake GCP service account is still missing IAM permissions on the bucket.

## Step 8: Run the full pipeline

Run:

```bash
bash pipeline.sh
```

If you want the latest local CSVs to overwrite older files already in GCS, run:

```bash
FORCE_DATASET_UPLOAD=1 bash pipeline.sh
```

What `pipeline.sh` does:

| Stage | Action |
|---|---|
| 1 | Loads `.env` |
| 2 | Checks local prerequisites (`gcloud`, `python3`) |
| 3 | Ensures required local Python packages are available |
| 4 | Detects/validates required local datasets |
| 5 | Generates `historical_zip_population_10yrs.csv` if missing (default path mode) |
| 6 | Verifies access to your GCS bucket |
| 7 | Uploads required datasets to canonical GCS object names |
| 8 | Verifies Snowflake stage access with `LIST @stage` |
| 9 | Creates Dataproc cluster if missing |
| 10 | Submits the Spark ETL job |
| 11 | Loads curated output into Snowflake |
| 12 | Builds `FINAL_TABLE` and downstream views |

## What to expect after a successful run

In Snowflake, you should see:

- `CHICAGO_ANALYTICAL_TABLE`
- `CHICAGO_POPULATION`
- `FINAL_TABLE`
- several `vw_*` views
- staged Spark output in `gs://YOUR_BUCKET_NAME/pipeline.csv/`

Tableau dashboard:

- [Chicago Crime & Housing Analysis Dashboard](https://public.tableau.com/app/profile/xi.long/viz/Chicago_Crime_Housing_Analysis_UCLA_Project/PropertyCrimeCountDistribution?publish=yes)

## Important data rule in this repo

This project now enforces a `2014-2024` analysis window.

That means:

- the Spark output is filtered to `2014-2024`
- `join_all.sql` only joins `2014-2024`
- if there is no valid overlap between analytical data and population data in that range, the pipeline will fail instead of silently creating an empty `FINAL_TABLE`

## Common errors and fixes

### Error: Stage does not exist or not authorized

Example:

```text
Stage 'CHICAGO_HOUSING_PROJECT.PUBLIC.GCP_CHICAGO_STAGE' does not exist or not authorized
```

Fix:

- create the stage in Snowflake
- make sure `SNOWFLAKE_STAGE` in `.env` matches the actual stage name
- make sure the role in `.env` has `USAGE` on the stage

### Error: Location is not allowed by integration

Example:

```text
Location 'gcs://YOUR_BUCKET/' is not allowed by integration GCP_INTEGRATION
```

Fix:

```sql
ALTER STORAGE INTEGRATION gcs_crime_integration
SET STORAGE_ALLOWED_LOCATIONS = ('gcs://YOUR_BUCKET/');
```

### Error: does not have `storage.objects.list` access

Example:

```text
Failure using stage area. Cause: [service-account] does not have storage.objects.list access
```

Fix:

- run `DESC INTEGRATION gcs_crime_integration`
- copy the `STORAGE_GCP_SERVICE_ACCOUNT`
- grant that service account `roles/storage.objectViewer` on the bucket

### Error: `FINAL_TABLE` is empty

Likely cause:

- your analytical output and population data do not overlap on `ZIP_CODE + YEAR`

Current guardrail:

- the pipeline now fails early if there are no overlapping `2014-2024` ZIP/year pairs

### Error: local CSV file not found

Fix:

- put the files in the repo root, or
- set `LOCAL_CRIME_FILE`, `LOCAL_ZILLOW_FILE`, `LOCAL_BOUNDARY_FILE`, and `LOCAL_POPULATION_FILE` in `.env`

## Useful manual checks

Check the bucket:

```bash
gcloud storage ls gs://YOUR_BUCKET_NAME
```

Check the stage:

```sql
LIST @gcp_chicago_stage;
```

Check the integration:

```sql
DESC INTEGRATION gcs_crime_integration;
```

## Files to know

| File | Role in pipeline |
|---|---|
| [pipeline.sh](pipeline.sh) | End-to-end orchestration (checks, upload, Dataproc submit, Snowflake load) |
| [population_data.py](population_data.py) | Generates `historical_zip_population_10yrs.csv` |
| [fetch_population_data.py](fetch_population_data.py) | Census API fetch implementation used by `population_data.py` |
| [assets/setup_snowflake_stage.sql](assets/setup_snowflake_stage.sql) | One-time Snowflake integration/stage setup |
| [assets/spark_job.py](assets/spark_job.py) | PySpark + Sedona spatial ETL job |
| [assets/load_from_GCS.sql](assets/load_from_GCS.sql) | Loads staged data from `pipeline.csv/` in GCS into Snowflake tables |
| [assets/join_all.sql](assets/join_all.sql) | Builds final `FINAL_TABLE` model |

# Spatiotemporal-Analysis-of-Chicago-Crime-and-Housing-Value-Trends

