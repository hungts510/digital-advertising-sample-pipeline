{{
  config(
    materialized='table',
    description='Simplified staging model that directly reads from your existing staged parquet data'
  )
}}

WITH base_data AS (
    -- Direct read from your existing staged parquet file
    SELECT 
        date,
        domain,
        format,
        country,
        ad_size,
        device,
        adSelectionEmissions,
        creativeDistributionEmissions,
        mediaDistributionEmissions,
        totalEmissions,
        domainCoverage
    FROM parquet.`s3a://sample-bucket/staging/advertising_emissions.parquet`
),

cleaned_data AS (
    SELECT
        -- Date standardization
        CAST(date AS DATE) AS event_date,
        
        -- String cleaning and standardization
        LOWER(TRIM(domain)) AS domain,
        LOWER(TRIM(format)) AS ad_format,
        UPPER(TRIM(country)) AS country_code,
        TRIM(ad_size) AS ad_size,
        LOWER(TRIM(device)) AS device_type,
        
        -- Emissions data with quality checks
        CAST(adSelectionEmissions AS DECIMAL(10,6)) AS ad_selection_emissions,
        CAST(creativeDistributionEmissions AS DECIMAL(10,6)) AS creative_distribution_emissions,
        CAST(mediaDistributionEmissions AS DECIMAL(10,6)) AS media_distribution_emissions,
        CAST(totalEmissions AS DECIMAL(10,6)) AS total_emissions,
        
        -- Domain coverage handling
        CASE 
            WHEN domainCoverage IS NULL OR domainCoverage = '' THEN 'unknown'
            ELSE LOWER(TRIM(domainCoverage))
        END AS domain_coverage,
        
        -- Basic data quality flags
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
        
        -- Record metadata
        CURRENT_TIMESTAMP() AS processed_at
        
    FROM base_data
    WHERE 
        -- Basic filters
        date IS NOT NULL
        AND domain IS NOT NULL
        AND domain != ''
        AND format IS NOT NULL
        AND country IS NOT NULL
        AND device IS NOT NULL
)

SELECT 
    *,
    -- Emission percentage calculations
    CASE 
        WHEN total_emissions != 0 THEN 
            ROUND((ad_selection_emissions / total_emissions) * 100, 2)
        ELSE 0 
    END AS ad_selection_pct,
    
    CASE 
        WHEN total_emissions != 0 THEN 
            ROUND((creative_distribution_emissions / total_emissions) * 100, 2)
        ELSE 0 
    END AS creative_distribution_pct,
    
    CASE 
        WHEN total_emissions != 0 THEN 
            ROUND((media_distribution_emissions / total_emissions) * 100, 2)
        ELSE 0 
    END AS media_distribution_pct

FROM cleaned_data 