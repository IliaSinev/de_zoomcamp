-- Question 3: Count records
-- How many taxi trips were there on January 15?

SELECT 
	COUNT(*)
FROM public.yellow_taxi_trips
WHERE 1=1
	AND date_part('month', tpep_pickup_datetime) = 1
	AND date_part('day', tpep_pickup_datetime) = 15;