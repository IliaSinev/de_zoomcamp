--Question 4: Largest tip for each day
--On which day it was the largest tip in January? (note: it's not a typo, it's "tip", not "trip")

SELECT 
	tpep_pickup_datetime
FROM public.yellow_taxi_trips
WHERE 1=1
	AND tip_amount = 
		(SELECT MAX(tip_amount) FROM public.yellow_taxi_trips);