-- QUESTION: What was the most popular destination for passengers picked up in central park on January 14? 
-- Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

SELECT 
	COUNT(*) AS CNT,
	COALESCE(lkp2."Zone", 'Unknown') AS "Drop-off Name"	
FROM public.yellow_taxi_trips AS tr
-- Loaded the Lookup table from the same source
LEFT OUTER JOIN public.zone_lookup AS lkp1
	ON tr."PULocationID" = lkp1."LocationID"
-- Could not think of a better solution without second join...
LEFT OUTER JOIN public.zone_lookup AS lkp2
	ON tr."DOLocationID" = lkp2."LocationID"
WHERE 1=1
	AND lkp1."Zone" = 'Central Park'
	AND date_part('month', tr.tpep_pickup_datetime) = 1
	AND date_part('day', tr.tpep_pickup_datetime) = 14
GROUP BY 
	  lkp2."Zone"
ORDER BY CNT DESC
LIMIT 1;