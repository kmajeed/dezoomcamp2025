-- create enternal table in big query
CREATE OR REPLACE EXTERNAL TABLE `taxi_data.yellow_taxi_2024`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-01.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-02.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-03.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-04.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-05.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-06.parquet',
    'gs://kmajeeddev_hw3_2025/yellow_tripdata_2024-07.parquet'
  ]
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `taxi_data.yellow_taxi_2024_non_partitoned` AS
SELECT *
FROM `taxi_data.yellow_taxi_2024`; 

-- Distinct number of pickup locations
-- For the External Table:
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids_external
FROM `taxi_data.yellow_taxi_2024`;

-- For the Materialized Table:
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocationids_materialized
FROM `taxi_data.yellow_taxi_2024_non_partitoned`; 

-- Retrieve only PULocationID
SELECT PULocationID
FROM `taxi_data.yellow_taxi_2024_non_partitoned`;  

-- Retrieve PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM `taxi_data.yellow_taxi_2024_non_partitoned`;

-- Records with zdero fare
SELECT COUNT(*) AS count_fare_amount_zero
  FROM `taxi_data.yellow_taxi_2024_non_partitoned`
 WHERE fare_amount = 0;

 -- create partitioned table
CREATE OR REPLACE TABLE `taxi_data.yellow_taxi_2024_partitoned`
PARTITION BY DATE(tpep_dropoff_datetime)  -- Partition by date
CLUSTER BY VendorID                   -- Cluster by VendorID
AS
SELECT *
FROM `taxi_data.yellow_taxi_2024_non_partitoned`; 

--Distinct Vendors between two dates using non optimized table
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_2024_non_partitoned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01 00:00:00' AND '2024-03-15 23:59:59';

-- Distinct Vendors between two dates using optimized table
SELECT DISTINCT VendorID
FROM `taxi_data.yellow_taxi_2024_partitoned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01 00:00:00' AND '2024-03-15 23:59:59';