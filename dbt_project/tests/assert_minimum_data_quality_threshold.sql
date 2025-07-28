-- Test that at least 95% of records meet minimum data quality threshold
-- Fails if more than 5% of records have quality score below 50

WITH quality_stats AS (
    SELECT 
        COUNT(*) AS total_records,
        COUNT(CASE WHEN data_quality_score >= 50 THEN 1 END) AS good_quality_records,
        ROUND(
            COUNT(CASE WHEN data_quality_score >= 50 THEN 1 END) * 100.0 / COUNT(*), 
            2
        ) AS good_quality_percentage
    FROM {{ ref('stg_advertising_emissions') }}
)

SELECT 
    total_records,
    good_quality_records,
    good_quality_percentage,
    'Data quality threshold not met' AS failure_reason
FROM quality_stats
WHERE good_quality_percentage < 95.0  -- Fail if less than 95% meet threshold 