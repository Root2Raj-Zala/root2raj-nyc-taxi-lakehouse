import tempfile,unittest
from pathlib import Path
import duckdb
import pandas as pd
from pipeline import ROOT,REQUIRED,validate_contract,build

class WarehouseTests(unittest.TestCase):
    def test_schema_drift_fails_closed(self):
        with self.assertRaises(ValueError): validate_contract(REQUIRED-{'fare_amount'})
    def test_reject_routing_and_replay(self):
        cache=ROOT/'.cache'/'tests'; cache.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(dir=cache) as folder:
            folder=Path(folder)
            base={'tpep_pickup_datetime':pd.Timestamp('2024-01-15 10:00'),'tpep_dropoff_datetime':pd.Timestamp('2024-01-15 10:20'),
                  'PULocationID':1,'DOLocationID':1,'trip_distance':3.0,'fare_amount':12.0,'total_amount':15.0,'payment_type':1}
            rows=[base,{**base,'trip_distance':0.},{**base,'fare_amount':-12.},
                  {**base,'tpep_pickup_datetime':pd.Timestamp('2023-12-31')},
                  {**base,'PULocationID':999},{**base,'tpep_dropoff_datetime':base['tpep_pickup_datetime']}]
            source=folder/'trips.parquet'; pd.DataFrame(rows).to_parquet(source,index=False)
            zones=folder/'zones.csv'; pd.DataFrame({'LocationID':[1],'Borough':['Test'],'Zone':['Fixture'],'service_zone':['Fixture']}).to_csv(zones,index=False)
            con=duckdb.connect(); first=build(con,source,zones,'test-fixture'); second=build(con,source,zones,'test-fixture')
            self.assertEqual(first,second)
            self.assertEqual(con.execute('SELECT count(*) FROM fact_trip').fetchone()[0],1)
            self.assertEqual(con.execute('SELECT count(*) FROM quarantine').fetchone()[0],5)
            self.assertEqual(con.execute('SELECT record_key FROM fact_trip').fetchone()[0],'test-fixture:0')
            con.close()

if __name__=='__main__': unittest.main()
