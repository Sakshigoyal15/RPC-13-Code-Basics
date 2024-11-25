USE targets_db;
USE trips_db;
-- 1. City Level Fair and Trip Summary Report

/*
Fields Name: 1. city_name
             2. total_trips
             3. avg_fare_per_km
             4. avg_fare_per_trips
             5. %_contribution_to_total_trips
*/

WITH raw_data AS (
SELECT c.city_name,
       COUNT(DISTINCT trip_id) AS total_trips,
       SUM(distance_travelled_km) AS total_distance_travelled,
       SUM(fare_amount) AS total_fare
FROM trips_db.fact_trips t
LEFT JOIN trips_db.dim_city c 
ON t.city_id = c.city_id
GROUP BY 1
)
SELECT city_name,
       total_trips,
       total_fare/total_distance_travelled AS avg_fare_per_km,
       total_fare/total_trips AS avg_fare_per_trips,
       100.0 * total_trips/SUM(total_trips) OVER() AS pct_contribution_to_total_trips
FROM raw_data
ORDER BY 5 DESC;

-- 2. Monthly City-Level Trips Target Performance Report

/*
Fields Name : 1. city_name
              2. month_name
              3. actual_trips
              4. target_trips
              5. performance_status
              6. %-difference
*/

-- 3. City Level Repeat Passenger Trip Frequency Report

/*
city_name, 2_trips, 3_trips, 4-trips, .... , 10-Trips
*/

WITH raw_trips_freq AS (
	SELECT city_id,
		   trip_count,
		   SUM(repeat_passenger_count) AS repeat_passenger_count
	FROM trips_db.dim_repeat_trip_distribution
	-- WHERE city_id = 'AP01' 
	GROUP BY 1,2
    )
    -- SELECT * FROM raw_trips_freq ORDER BY city_id;  
    ,
    pivot_data AS (
    SELECT city_id,
           SUM(CASE WHEN trip_count = '2-Trips' THEN repeat_passenger_count ELSE NULL END) AS '2_Trips',
           SUM(CASE WHEN trip_count = '3-Trips' THEN repeat_passenger_count ELSE NULL END) AS '3_Trips',
           SUM(CASE WHEN trip_count = '4-Trips' THEN repeat_passenger_count ELSE NULL END) AS '4_Trips',
           SUM(CASE WHEN trip_count = '5-Trips' THEN repeat_passenger_count ELSE NULL END) AS '5_Trips',
           SUM(CASE WHEN trip_count = '6-Trips' THEN repeat_passenger_count ELSE NULL END) AS '6_Trips',
           SUM(CASE WHEN trip_count = '7-Trips' THEN repeat_passenger_count ELSE NULL END) AS '7_Trips',
           SUM(CASE WHEN trip_count = '8-Trips' THEN repeat_passenger_count ELSE NULL END) AS '8_Trips',
           SUM(CASE WHEN trip_count = '9-Trips' THEN repeat_passenger_count ELSE NULL END) AS '9_Trips',
           SUM(CASE WHEN trip_count = '10-Trips' THEN repeat_passenger_count ELSE NULL END) AS '10_Trips',
           SUM(repeat_passenger_count) AS total_passenger
    FROM raw_trips_freq
    GROUP BY 1
    )
    -- SELECT * FROM pivot_data;
    SELECT -- pd.city_id,
           c.city_name,
		   ROUND(100.0 * 2_Trips/total_passenger,1) AS '2-Trips',
           ROUND(100.0 * 3_Trips/total_passenger,1) AS '3-Trips',
           ROUND(100.0 * 4_Trips/total_passenger,1) AS '4-Trips',
           ROUND(100.0 * 5_Trips/total_passenger,1) AS '5-Trips',
           ROUND(100.0 * 6_Trips/total_passenger,1) AS '6-Trips',
           ROUND(100.0 * 7_Trips/total_passenger,1) AS '7-Trips',
           ROUND(100.0 * 8_Trips/total_passenger,1) AS '8-Trips',
           ROUND(100.0 * 9_Trips/total_passenger,1) AS '9-Trips',
           ROUND(100.0 * 10_Trips/total_passenger,1) AS '10-Trips'
    FROM pivot_data pd
    LEFT JOIN trips_db.dim_city c
    ON c.city_id = pd.city_id;


WITH raw_report AS (
	SELECT c.city_name,
		   EXTRACT(Month FROM t.date) AS month_, 
		   MONTHNAME(t.date) AS month,
		   mt.total_target_trips AS target_trips,
		   COUNT(DISTINCT t.trip_id) AS Actual_Trips
	FROM trips_db.fact_trips t 
	LEFT JOIN trips_db.dim_city c
	ON t.city_id = c.city_id
	LEFT JOIN targets_db.monthly_target_trips mt
	ON mt.city_id = t.city_id
	AND MONTHNAME(mt.month) = MONTHNAME(t.date)
	GROUP BY 1,2,3,4
    ORDER BY 1,2
    )
    SELECT city_name,
		   month,
           target_trips,
           Actual_Trips,
           CASE WHEN Actual_Trips > target_trips THEN 'Above Target'
		WHEN Actual_Trips <= target_trips THEN 'Below Target'
           END AS performance_status,
           CONCAT(ROUND(100.0 * (Actual_Trips - target_trips)/target_trips),' %') AS pct_difference
    FROM raw_report;
    
-- 4. Identify Cities with Total New Passengers and Categorize them

WITH new_passenger_summary AS (
	SELECT 
		city_id,
		SUM(new_passengers) AS total_new_passengers,
              RANK() OVER(ORDER BY SUM(new_passengers) DESC) AS top_rnk,
              RANK() OVER(ORDER BY SUM(new_passengers)) AS lower_rnk
	FROM
		trips_db.fact_passenger_summary
	GROUP BY 1
    ORDER BY total_new_passengers DESC
    )
    ,
    final AS (
    SELECT city_id,
		   total_new_passengers,
           CASE WHEN top_rnk <= 3 THEN 'Top-3'
		   ELSE NULL END AS city_category
    FROM new_passenger_summary
    WHERE top_rnk <= 3
    UNION ALL
    SELECT city_id,
		   total_new_passengers,
           CASE WHEN lower_rnk <= 3 THEN 'Bottom-3'
	       ELSE NULL 
              END AS city_category
    FROM new_passenger_summary
    WHERE lower_rnk <= 3
    )
    SELECT city_name,
           total_new_passengers,
           city_category
    FROM final f 
    LEFT JOIN trips_db.dim_city c
    USING (city_id);
    
-- 5. Identify Month with Highest Revenue for Each City

/*
city_name
Highest_Revenue_Month,
Revenue,
percentage_contribution(%)
*/

WITH city_revenue AS (
	SELECT city_id,
		   MONTHNAME(date) AS month,
		   EXTRACT(Month FROM date) AS month_num,
		   SUM(fare_amount) AS revenue,
           RANK() OVER(PARTITION BY city_id ORDER BY SUM(fare_amount) DESC) AS rnk
	FROM trips_db.fact_trips
	GROUP BY 1,2,3
	ORDER BY 1,3
    )
    ,
    month_contribution AS (
    SELECT city_id,
	       month,
              month_num,
              revenue,
              rnk,
		SUM(revenue) OVER(PARTITION BY city_id) AS cumsum_revenue
    FROM city_revenue
    )
    SELECT city_name,
           month,
           revenue,
           ROUND(100.0 * revenue/cumsum_revenue,0) AS 'percentage_contribution(%)'
    FROM month_contribution mc 
    LEFT JOIN trips_db.dim_city c
    USING (city_id)
    WHERE rnk = 1;
    
-- 6. Repeat Passenger Rate Analysis

/*
city_name,
month,
total_passengers,
repeat_passengers,
monthly_repeat_passenger_rate(%)
city_repeat_passenger_rate(%)
*/

WITH raw_month AS (
	SELECT MONTHNAME(month) AS month,
              EXTRACT(Month FROM month) AS month_num,
		city_id,
		SUM(total_passengers) AS total_passengers,
		SUM(repeat_passengers) AS repeat_passengers
	FROM trips_db.fact_passenger_summary
	GROUP BY 1,2,3
    ORDER BY 3,2
    )
    ,
    monthly_repeat_rates AS (
    SELECT city_id,
		month,
              total_passengers,
              repeat_passengers,
              ROUND(100.0 * repeat_passengers/total_passengers) AS monthly_repeat_passenger_rate
    FROM raw_month
    )
    ,
    city_repeat_rates AS (
    SELECT city_id,
           SUM(total_passengers) AS total_passengers,
           SUM(repeat_passengers) AS repeat_passengers,
           ROUND(100.0 * SUM(repeat_passengers) / SUM(total_passengers)) AS city_repeat_passenger_rate
	FROM raw_month
    GROUP BY 1
    )
    SELECT city_name,
           m.month,
           m.total_passengers,
           m.repeat_passengers,
           m.monthly_repeat_passenger_rate,
           c.city_repeat_passenger_rate
    FROM monthly_repeat_rates m
    LEFT JOIN city_repeat_rates c
    USING (city_id)
    LEFT JOIN trips_db.dim_city dc 
    USING (city_id);
