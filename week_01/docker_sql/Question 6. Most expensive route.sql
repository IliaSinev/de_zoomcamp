--Question 6: Most expensive route
--What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)? 
--Enter two zone names separated by a slashFor example:"Jamaica Bay / Clinton East"If any of the zone names are unknown (missing), 
--write "Unknown". For example, "Unknown / Clinton East".

SELECT 
	AVG(total_amount) AS "Average price",
	COALESCE(lkp1."Zone", 'Unknown') ||' / ' || COALESCE(lkp2."Zone", 'Unknown') AS "Zones"
FROM public.yellow_taxi_trips AS tr
LEFT OUTER JOIN public.zone_lookup AS lkp1
	ON tr."PULocationID" = lkp1."LocationID"
-- Could not think of a better solution without second join...
LEFT OUTER JOIN public.zone_lookup AS lkp2
	ON tr."DOLocationID" = lkp2."LocationID"
GROUP BY COALESCE(lkp1."Zone", 'Unknown') ||' / ' || COALESCE(lkp2."Zone", 'Unknown')
ORDER BY "Average price" DESC
LIMIT 1;