WITH constructor_metrics AS
( 
    SELECT constructor_name, 
           SUM(race_starts) AS race_starts, 
           SUM (number_of_wins) AS total_wins,
           SUM (number_of_podiums) AS total_podiums,
           SUM((CASE WHEN standing = 1 THEN 1 ELSE 0 END)) AS total_championships
    FROM formula1.gold.v_constructor_standing
    GROUP BY constructor_name
    HAVING total_championships >=1
)
SELECT constructor_name, 
race_starts, 
total_wins,
total_podiums,
total_championships,
(total_championships * 100) + (total_wins * 10) + (total_podiums * 3) AS greatness_score
FROM constructor_metrics
ORDER BY greatness_score desc