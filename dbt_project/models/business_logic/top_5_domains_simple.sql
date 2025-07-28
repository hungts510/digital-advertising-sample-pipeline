{{
  config(
    materialized='table',
    description='Top 5 domains by total emissions - simplified version'
  )
}}

WITH domain_totals AS (
    SELECT
        domain,
        SUM(totalEmissions) AS total_emissions,
        COUNT(*) AS total_ads,
        ROUND(AVG(totalEmissions), 6) AS avg_emissions_per_ad,
        COUNT(DISTINCT country) AS countries_served,
        COUNT(DISTINCT format) AS formats_used,
        COUNT(DISTINCT device) AS device_types_targeted
    FROM parquet.`s3a://sample-bucket/staging/advertising_emissions.parquet`
    GROUP BY domain
),

ranked_domains AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY total_emissions DESC) AS rank
    FROM domain_totals
)

SELECT 
    rank,
    domain,
    total_emissions,
    total_ads,
    avg_emissions_per_ad,
    countries_served,
    formats_used,
    device_types_targeted,
    CURRENT_TIMESTAMP() AS generated_at

FROM ranked_domains
WHERE rank <= 5
ORDER BY rank 