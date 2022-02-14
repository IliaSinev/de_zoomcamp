-- SQL Query to create For Hire Vehicle Taxi table partitioned by pick up date and clustered by Base License Number

CREATE OR REPLACE TABLE `datatalks-de-zoomcamp.trips_data_all.fhv_tripdata_all_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `datatalks-de-zoomcamp.trips_data_all.fhv_external`;

-- Query partitioned-clustered table filtered by the date and license number

select count(*)
from datatalks-de-zoomcamp.trips_data_all.fhv_tripdata_all_partitioned_clustered
where 1=1
and date(pickup_datetime) between date('2019-01-01') and date('2019-03-31')
and dispatching_base_num in ('B00987', 'B02060', 'B02279')

-- Estimated data processed: 400MiB, actual data processed 139.8 MiB

-- Same query on the external table:

select count(*)
from datatalks-de-zoomcamp.trips_data_all.fhv_external
where 1=1
and date(pickup_datetime) between date('2019-01-01') and date('2019-03-31')
and dispatching_base_num in ('B00987', 'B02060', 'B02279')

-- Actual data processed: 2.2 GiB
