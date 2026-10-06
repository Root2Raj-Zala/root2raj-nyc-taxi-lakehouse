"""Real NYC trip warehouse: immutable source, explicit quarantine, atomic replacement."""
from pathlib import Path
import hashlib, json, urllib.request
import duckdb
ROOT = Path(__file__).resolve().parent
TRIPS_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
ZONES_URL = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
REQUIRED = {'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID',
            'trip_distance', 'fare_amount', 'total_amount', 'payment_type'}

def validate_contract(columns):
    missing = REQUIRED - set(columns)
    if missing: raise ValueError('Trip schema missing required columns: ' + ', '.join(sorted(missing)))

def sha256(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def prepare_sources():
    data = ROOT / 'data'; data.mkdir(exist_ok=True)
    for url, filename in [(TRIPS_URL, 'yellow_tripdata_2024-01.parquet'), (ZONES_URL, 'taxi_zone_lookup.csv')]:
        destination = data / filename
        if not destination.exists():
            temporary = destination.with_suffix('.download')
            urllib.request.urlretrieve(url, temporary)
            temporary.replace(destination)
    return data / 'yellow_tripdata_2024-01.parquet', data / 'taxi_zone_lookup.csv'

def build(con, source, zones, source_sha):
    # Views expose the immutable local source. Missing-schema failure occurs before replacing tables.
    source_sql = str(source).replace(chr(39), chr(39) * 2)
    con.execute("CREATE OR REPLACE TEMP VIEW raw_trips AS SELECT * FROM read_parquet('" + source_sql + "', file_row_number=true)")
    validate_contract([row[0] for row in con.execute('DESCRIBE raw_trips').fetchall()])
    con.execute('CREATE OR REPLACE TEMP TABLE new_zones AS SELECT LocationID::INTEGER AS zone_id, coalesce(Borough, \'Unknown\') AS borough, Zone AS zone_name, service_zone FROM read_csv_auto(?)', [str(zones)])
    con.execute("""CREATE OR REPLACE TEMP TABLE staged AS
    SELECT file_row_number AS source_row, *,
      CASE WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
             OR trip_distance IS NULL OR fare_amount IS NULL OR total_amount IS NULL THEN 'missing_required'
           WHEN tpep_pickup_datetime < TIMESTAMP '2024-01-01' OR tpep_pickup_datetime >= TIMESTAMP '2024-02-01' THEN 'outside_pickup_month'
           WHEN date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) <= 0
             OR date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) > 14400 THEN 'duration_policy'
           WHEN trip_distance <= 0 OR trip_distance > 100 THEN 'distance_policy'
           WHEN fare_amount < 0 OR total_amount < 0 THEN 'negative_financial_record'
           WHEN PULocationID NOT IN (SELECT zone_id FROM new_zones)
             OR DOLocationID NOT IN (SELECT zone_id FROM new_zones)
             OR PULocationID IS NULL OR DOLocationID IS NULL THEN 'unmapped_zone'
           ELSE 'accepted' END AS quality_status FROM raw_trips""")
    # Atomic full-month replacement avoids double counting on retries; no unsupported trip deduplication.
    con.execute('BEGIN TRANSACTION')
    try:
        for table in ['fact_trip', 'dim_date', 'dim_zone', 'quarantine', 'load_manifest']:
            con.execute(f'DROP TABLE IF EXISTS {table}')
        con.execute('CREATE TABLE dim_zone AS SELECT * FROM new_zones')
        con.execute('CREATE UNIQUE INDEX zone_pk ON dim_zone(zone_id)')
        con.execute("CREATE TABLE dim_date AS SELECT d::DATE AS date_key, dayofweek(d) AS weekday, day(d) AS day_of_month FROM generate_series(DATE '2024-01-01', DATE '2024-01-31', INTERVAL '1 day') t(d)")
        con.execute("""CREATE TABLE fact_trip AS SELECT
          ? || ':' || source_row::VARCHAR AS record_key,
          tpep_pickup_datetime::DATE AS date_key, tpep_pickup_datetime AS pickup_at,
          tpep_dropoff_datetime AS dropoff_at, PULocationID::INTEGER AS pickup_zone_id,
          DOLocationID::INTEGER AS dropoff_zone_id, payment_type::INTEGER AS payment_type,
          trip_distance::DOUBLE AS miles,
          fare_amount::DECIMAL(18,2) AS fare_usd, total_amount::DECIMAL(18,2) AS total_usd,
          date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) AS duration_seconds
          FROM staged WHERE quality_status='accepted'""", [source_sha])
        con.execute('CREATE UNIQUE INDEX trip_pk ON fact_trip(record_key)')
        con.execute("CREATE TABLE quarantine AS SELECT source_row, quality_status, tpep_pickup_datetime, trip_distance, fare_amount, total_amount FROM staged WHERE quality_status<>'accepted'")
        con.execute("CREATE TABLE load_manifest AS SELECT ? AS source_sha256, ? AS zones_sha256, count(*) AS source_rows FROM staged", [source_sha, sha256(zones)])
        con.execute((ROOT / 'sql' / 'marts.sql').read_text(encoding='utf-8-sig'))
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK'); raise
    rows = con.execute('SELECT * FROM gold_daily ORDER BY date_key').fetchall()
    normalized = [[round(v, 6) if isinstance(v, float) else v for v in row] for row in rows]
    digest = hashlib.sha256(json.dumps(normalized, default=str).encode()).hexdigest()
    return digest

def main():
    source, zones = prepare_sources()
    output = ROOT / 'outputs'; output.mkdir(exist_ok=True)
    con = duckdb.connect(str(output / 'warehouse.duckdb'))
    con.execute("SET temp_directory = '" + str(ROOT / '.cache' / 'duckdb').replace('\\', '/') + "'")
    source_sha = sha256(source)
    first_digest = build(con, source, zones, source_sha)
    second_digest = build(con, source, zones, source_sha)
    counts = dict(con.execute('SELECT quality_status, count(*) FROM staged GROUP BY 1').fetchall())
    accepted = counts.get('accepted', 0); total = sum(counts.values())
    result = con.execute('SELECT sum(trips), sum(total_usd), sum(miles) FROM gold_daily').fetchone()
    base = con.execute('SELECT count(*), sum(total_usd), sum(miles) FROM fact_trip').fetchone()
    checks = {'schema_contract_passed': True, 'source_partition_reconciles': total == con.execute('SELECT count(*) FROM raw_trips').fetchone()[0],
              'fact_count_equals_accepted': base[0] == accepted, 'unique_fact_record_keys': con.execute('SELECT count(DISTINCT record_key) FROM fact_trip').fetchone()[0] == accepted,
              'zone_dimension_unique': con.execute('SELECT count(*)=count(DISTINCT zone_id) FROM dim_zone').fetchone()[0],
              'no_orphan_zone_foreign_keys': con.execute('SELECT count(*) FROM fact_trip f LEFT JOIN dim_zone p ON f.pickup_zone_id=p.zone_id LEFT JOIN dim_zone d ON f.dropoff_zone_id=d.zone_id WHERE p.zone_id IS NULL OR d.zone_id IS NULL').fetchone()[0] == 0,
              'gold_trip_and_currency_reconcile': result[:2] == base[:2],
              'gold_distance_reconciles': abs(result[2] - base[2]) < .001,
              'repeat_load_is_idempotent': first_digest == second_digest,
              'pickup_dates_in_month': con.execute("SELECT count(*) FROM fact_trip WHERE date_key < DATE '2024-01-01' OR date_key >= DATE '2024-02-01'").fetchone()[0] == 0}
    assert all(checks.values()), checks
    for table in ['gold_daily', 'gold_pickup_borough']:
        con.execute(f"COPY {table} TO '" + str(output / (table + '.csv')).replace('\\', '/') + "' (HEADER, DELIMITER ',')")
    con.execute("COPY (SELECT * FROM fact_trip ORDER BY record_key) TO '" + str(output / 'silver_trips.parquet').replace('\\', '/') + "' (FORMAT PARQUET, COMPRESSION ZSTD)")
    summary = {'source_url': TRIPS_URL, 'zones_url': ZONES_URL, 'source_sha256': source_sha,
               'zones_sha256': sha256(zones), 'source_rows': total, 'accepted_rows': accepted,
               'quarantined_rows': total - accepted, 'quality_partition': counts,
               'accepted_total_amount_usd': str(base[1]), 'accepted_distance_miles': float(base[2]),
               'dimension_zone_rows': con.execute('SELECT count(*) FROM dim_zone').fetchone()[0],
               'gold_digest': first_digest, 'idempotency_verified_runs': 2,
               'top_pickup_boroughs': con.execute('SELECT borough, trips FROM gold_pickup_borough ORDER BY trips DESC').fetchall(),
               'scope': 'Full January 2024 file; pickup-cohort operational mart. Quarantine is not proof of fraudulent records. Total amount is not profit; cash tips absent.'}
    (output / 'metrics.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    (output / 'validation.json').write_text(json.dumps(checks, indent=2), encoding='utf-8')
    plan = con.execute("EXPLAIN SELECT date_key,count(*) FROM fact_trip WHERE date_key=DATE '2024-01-15' GROUP BY 1").fetchall()
    (output / 'query_plan.txt').write_text('\n'.join(str(x[1]) for x in plan), encoding='utf-8')
    con.close()
    print(json.dumps(summary, indent=2))

if __name__ == '__main__': main()

