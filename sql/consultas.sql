

-- Total de registros por ciudad
SELECT city, COUNT(*) AS total
FROM weather_observations
GROUP BY city
ORDER BY total DESC;

-- Última lectura por ciudad
SELECT w.city, w.timestamp_utc, w.temperature_c, w.wind_speed_kmh
FROM weather_observations w
JOIN (
    SELECT city, MAX(timestamp_utc) AS max_ts
    FROM weather_observations
    GROUP BY city
) m
ON w.city = m.city AND w.timestamp_utc = m.max_ts
ORDER BY w.city;

-- Ranking por temperatura (última lectura)
SELECT w.city, w.timestamp_utc, w.temperature_c
FROM weather_observations w
JOIN (
    SELECT city, MAX(timestamp_utc) AS max_ts
    FROM weather_observations
    GROUP BY city
) m
ON w.city = m.city AND w.timestamp_utc = m.max_ts
WHERE w.temperature_c IS NOT NULL
ORDER BY w.temperature_c DESC;