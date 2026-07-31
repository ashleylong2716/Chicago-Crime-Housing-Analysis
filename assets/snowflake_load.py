import os
import sys
from pathlib import Path
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

TARGET_START_YEAR = 2014
TARGET_END_YEAR = 2024


def run_sql_file(cur, path: Path) -> None:
    sql = path.read_text()
    
    # Inject environment variables
    sql = sql.format(**os.environ)

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for stmt in statements:
        print(f"\nRunning {path.name}: {stmt[:100]}...\n")
        cur.execute(stmt)


def validate_join_window(cur) -> None:
    cur.execute(
        f"""
        WITH analytical AS (
            SELECT DISTINCT ZIP_CODE, YEAR
            FROM CHICAGO_ANALYTICAL_TABLE
            WHERE YEAR BETWEEN {TARGET_START_YEAR} AND {TARGET_END_YEAR}
        ),
        population AS (
            SELECT DISTINCT ZIP_CODE, YEAR
            FROM CHICAGO_POPULATION
            WHERE YEAR BETWEEN {TARGET_START_YEAR} AND {TARGET_END_YEAR}
        )
        SELECT
            (SELECT COUNT(*) FROM analytical),
            (SELECT COUNT(*) FROM population),
            (
                SELECT COUNT(*)
                FROM analytical a
                INNER JOIN population p
                    ON a.ZIP_CODE = p.ZIP_CODE
                   AND a.YEAR = p.YEAR
            ),
            (SELECT MIN(YEAR) FROM CHICAGO_ANALYTICAL_TABLE),
            (SELECT MAX(YEAR) FROM CHICAGO_ANALYTICAL_TABLE)
        """
    )
    analytical_pairs, population_pairs, overlapping_pairs, analytical_min_year, analytical_max_year = cur.fetchone()

    print(
        "\nSnowflake overlap check: "
        f"analytical_pairs={analytical_pairs}, "
        f"population_pairs={population_pairs}, "
        f"overlapping_pairs={overlapping_pairs}, "
        f"analytical_year_range={analytical_min_year}..{analytical_max_year}\n"
    )

    if overlapping_pairs == 0:
        raise RuntimeError(
            "No overlapping ZIP_CODE + YEAR pairs exist between CHICAGO_ANALYTICAL_TABLE "
            f"and CHICAGO_POPULATION for {TARGET_START_YEAR}-{TARGET_END_YEAR}. "
            f"Analytical year range is {analytical_min_year}..{analytical_max_year}. "
            "The Spark output must be rebuilt with the correct 2014-2024 window before join_all.sql can succeed."
        )


def main() -> None:

    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

    snowflake_dir = Path(__file__).resolve().parent.parent / "assets"

    try:
        with conn.cursor() as cur:

            print("\nLoading data from GCS into Snowflake...\n")
            try:
                run_sql_file(cur, snowflake_dir / "load_from_GCS.sql")
            except ProgrammingError as e:
                if "does not exist or not authorized" in str(e):
                    print(f"\n❌ ERROR: Snowflake stage not found or unauthorized.")
                    print(f"Please ensure you have created the storage integration and stage in Snowflake.")
                    print(f"Refer to assets/setup_snowflake_stage.sql and README.md for instructions.")
                    print(f"Original error: {e}\n")
                    sys.exit(1)
                raise

            print("\nRunning join queries...\n")
            validate_join_window(cur)
            run_sql_file(cur, snowflake_dir / "join_all.sql")

            print("\nCreating Tableau views...\n")
            run_sql_file(cur, snowflake_dir / "views.sql")

            print("\nSnowflake pipeline completed successfully\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
