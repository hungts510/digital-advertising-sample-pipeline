{{
  config(
    materialized='table',
    description='Daily summary of advertising emissions by country and device type'
  )
}}

SELECT
    event_date,
    country_code,
    device_type,
    
    -- COUNT measures
    COUNT(*) AS total_ad_impressions,
    COUNT(DISTINCT domain) AS unique_domains,
    COUNT(DISTINCT ad_format) AS unique_formats,
    
    -- SUM measures
    ROUND(SUM(total_emissions), 6) AS total_emissions_sum,
    ROUND(SUM(ad_selection_emissions), 6) AS ad_selection_emissions_sum,
    ROUND(SUM(creative_distribution_emissions), 6) AS creative_distribution_emissions_sum,
    ROUND(SUM(media_distribution_emissions), 6) AS media_distribution_emissions_sum,
    
    -- AVG measures
    ROUND(AVG(total_emissions), 6) AS total_emissions_avg,
    ROUND(AVG(ad_selection_emissions), 6) AS ad_selection_emissions_avg,
    ROUND(AVG(creative_distribution_emissions), 6) AS creative_distribution_emissions_avg,
    ROUND(AVG(media_distribution_emissions), 6) AS media_distribution_emissions_avg,
    
    -- Percentage calculations for emission types
    CASE 
        WHEN SUM(total_emissions) != 0 THEN 
            ROUND((SUM(ad_selection_emissions) / SUM(total_emissions)) * 100, 2)
        ELSE 0 
    END AS ad_selection_percentage,
    
    CASE 
        WHEN SUM(total_emissions) != 0 THEN 
            ROUND((SUM(creative_distribution_emissions) / SUM(total_emissions)) * 100, 2)
        ELSE 0 
    END AS creative_distribution_percentage,
    
    CASE 
        WHEN SUM(total_emissions) != 0 THEN 
            ROUND((SUM(media_distribution_emissions) / SUM(total_emissions)) * 100, 2)
        ELSE 0 
    END AS media_distribution_percentage,
    
    -- Data quality metrics
    ROUND(AVG(data_quality_score), 2) AS avg_data_quality_score,
    SUM(CASE WHEN has_null_emissions THEN 1 ELSE 0 END) AS records_with_null_emissions,
    SUM(CASE WHEN has_emissions_mismatch THEN 1 ELSE 0 END) AS records_with_emissions_mismatch,
    SUM(CASE WHEN has_extreme_emissions THEN 1 ELSE 0 END) AS records_with_extreme_emissions,
    
    -- Additional insights
    MIN(total_emissions) AS min_emissions,
    MAX(total_emissions) AS max_emissions,
    ROUND(STDDEV(total_emissions), 6) AS emissions_stddev,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS aggregated_at

FROM {{ ref('stg_advertising_emissions') }}

GROUP BY 
    event_date,
    country_code,
    device_type

ORDER BY 
    event_date DESC,
    total_emissions_sum DESC 