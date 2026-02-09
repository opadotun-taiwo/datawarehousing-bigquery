CREATE OR REPLACE EXTERNAL TABLE
  `gothic-sled-453213-i2.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://datawarehouse_hw3_2026/yellow_tripdata_2024-*.parquet']
);


select count(*) from gothic-sled-453213-i2.nytaxi.external_yellow_tripdata;

create or replace table gothic-sled-453213-i2.nytaxi.yellow_tripdata as 
select * from gothic-sled-453213-i2.nytaxi.external_yellow_tripdata;


select count(distinct PULocationID) from 
gothic-sled-453213-i2.nytaxi.external_yellow_tripdata;

select count(distinct PULocationID) from 
gothic-sled-453213-i2.nytaxi.yellow_tripdata;


CREATE OR REPLACE TABLE `gothic-sled-453213-i2.nytaxi.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `gothic-sled-453213-i2.nytaxi.external_yellow_tripdata`;

SELECT COUNT(DISTINCT VendorID) AS distinct_vendors
FROM `gothic-sled-453213-i2.nytaxi.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';


SELECT COUNT(DISTINCT VendorID) AS distinct_vendors
FROM `gothic-sled-453213-i2.nytaxi.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';






