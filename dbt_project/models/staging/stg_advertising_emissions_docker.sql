{{
  config(
    materialized='table',
    description='Staging model that works with your Docker Spark setup - reads from existing staged table'
  )
}}

-- This model assumes you create a temporary view from your staged data
-- Run this first in your Spark cluster:
-- spark.read.parquet("s3a://sample-bucket/staging/advertising_emissions.parquet").createOrReplaceTempView("staged_emissions")

SELECT
    -- Date standardization
    CAST(date AS DATE) AS event_date,
    
    -- String cleaning and standardization
    LOWER(TRIM(domain)) AS domain,
    LOWER(TRIM(format)) AS ad_format,
    UPPER(TRIM(country)) AS country_code,
    TRIM(ad_size) AS ad_size,
    LOWER(TRIM(device)) AS device_type,
    
    -- Emissions data
    CAST(adSelectionEmissions AS DECIMAL(10,6)) AS ad_selection_emissions,
    CAST(creativeDistributionEmissions AS DECIMAL(10,6)) AS creative_distribution_emissions,
    CAST(mediaDistributionEmissions AS DECIMAL(10,6)) AS media_distribution_emissions,
    CAST(totalEmissions AS DECIMAL(10,6)) AS total_emissions,
    
    -- Domain coverage handling
    CASE 
        WHEN domainCoverage IS NULL OR domainCoverage = '' THEN 'unknown'
        ELSE LOWER(TRIM(domainCoverage))
    END AS domain_coverage,
    
    -- Data quality flags
    CASE 
        WHEN adSelectionEmissions IS NULL 
            OR creativeDistributionEmissions IS NULL 
            OR mediaDistributionEmissions IS NULL 
            OR totalEmissions IS NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS has_null_emissions,
    
    -- Extract date components
    YEAR(date) AS year,
    MONTH(date) AS month,
    DAY(date) AS day,
    
    -- Emission percentage calculations
    CASE 
        WHEN totalEmissions != 0 THEN 
            ROUND((adSelectionEmissions / totalEmissions) * 100, 2)
        ELSE 0 
    END AS ad_selection_pct,
    
    CASE 
        WHEN totalEmissions != 0 THEN 
            ROUND((creativeDistributionEmissions / totalEmissions) * 100, 2)
        ELSE 0 
    END AS creative_distribution_pct,
    
    CASE 
        WHEN totalEmissions != 0 THEN 
            ROUND((mediaDistributionEmissions / totalEmissions) * 100, 2)
        ELSE 0 
    END AS media_distribution_pct,
    
    -- Record metadata
    CURRENT_TIMESTAMP() AS processed_at

FROM staged_emissions
WHERE 
    -- Basic filters
    date IS NOT NULL
    AND domain IS NOT NULL
    AND domain != ''
    AND format IS NOT NULL
    AND country IS NOT NULL
    AND device IS NOT NULL 