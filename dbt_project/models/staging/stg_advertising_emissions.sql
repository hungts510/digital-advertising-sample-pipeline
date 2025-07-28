{{
  config(
    materialized='table',
    description='Cleaned and standardized advertising emissions data with data quality checks'
  )
}}

WITH raw_data AS (
    SELECT * FROM parquet.`s3a://sample-bucket/staging/advertising_emissions.parquet`
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
        
        -- Data quality flags
        CASE 
            WHEN adSelectionEmissions IS NULL 
                OR creativeDistributionEmissions IS NULL 
                OR mediaDistributionEmissions IS NULL 
                OR totalEmissions IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS has_null_emissions,
        
        CASE 
            WHEN ABS(
                (adSelectionEmissions + creativeDistributionEmissions + mediaDistributionEmissions) - totalEmissions
            ) > 0.001 
            THEN TRUE 
            ELSE FALSE 
        END AS has_emissions_mismatch,
        
        CASE 
            WHEN totalEmissions < -5.0 OR totalEmissions > 20.0 
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_emissions,
        
        -- Calculated fields
        adSelectionEmissions + creativeDistributionEmissions + mediaDistributionEmissions AS calculated_total_emissions,
        
        -- Extract date components for analysis
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        DAYOFWEEK(date) AS day_of_week,
        DATE_FORMAT(date, 'yyyy-MM') AS year_month,
        
        -- Add row quality score (0-100)
        100 - (
            CASE WHEN has_null_emissions THEN 30 ELSE 0 END +
            CASE WHEN has_emissions_mismatch THEN 20 ELSE 0 END +
            CASE WHEN has_extreme_emissions THEN 25 ELSE 0 END +
            CASE WHEN domain IS NULL OR domain = '' THEN 15 ELSE 0 END +
            CASE WHEN country IS NULL OR country = '' THEN 10 ELSE 0 END
        ) AS data_quality_score,
        
        -- Record metadata
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ var("start_date") }}' AS processing_date_range_start,
        '{{ var("end_date") }}' AS processing_date_range_end
        
    FROM raw_data
    WHERE 
        -- Basic data quality filters
        date IS NOT NULL
        AND domain IS NOT NULL
        AND domain != ''
        AND format IS NOT NULL
        AND country IS NOT NULL
        AND device IS NOT NULL
),

final_data AS (
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
)

SELECT * FROM final_data

-- Data quality validation
-- Only include records that meet minimum quality standards
WHERE data_quality_score >= 50  -- Configurable threshold 