-- Date dimension table generated using Snowflake sequence
WITH date_spine AS (
    SELECT 
        DATEADD(day, SEQ4(), '2000-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT
    date_day AS date_id,
    EXTRACT(year FROM date_day) AS year,
    EXTRACT(month FROM date_day) AS month,
    EXTRACT(day FROM date_day) AS day,
    EXTRACT(quarter FROM date_day) AS quarter,
    EXTRACT(dayofweek FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'YYYY-MM') AS year_month,
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Month') AS month_name
FROM date_spine
WHERE date_day <= '2030-12-31'::DATE