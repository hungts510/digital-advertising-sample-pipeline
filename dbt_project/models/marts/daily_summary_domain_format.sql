{{
  config(
    materialized='table',
    description='Daily summary of advertising emissions by domain and format'
  )
}}

SELECT
    event_date,
    domain,
    ad_format,
    
    -- COUNT measures
    COUNT(*) AS total_ad_impressions,
    COUNT(DISTINCT country_code) AS countries_served,
    COUNT(DISTINCT device_type) AS device_types_used,
    
    -- SUM measures for totalEmissions
    ROUND(SUM(total_emissions), 6) AS total_emissions_sum,
    ROUND(SUM(ad_selection_emissions), 6) AS ad_selection_emissions_sum,
    ROUND(SUM(creative_distribution_emissions), 6) AS creative_distribution_emissions_sum,
    ROUND(SUM(media_distribution_emissions), 6) AS media_distribution_emissions_sum,
    
    -- AVG measures for totalEmissions
    ROUND(AVG(total_emissions), 6) AS total_emissions_avg,
    ROUND(AVG(ad_selection_emissions), 6) AS ad_selection_emissions_avg,
    ROUND(AVG(creative_distribution_emissions), 6) AS creative_distribution_emissions_avg,
    ROUND(AVG(media_distribution_emissions), 6) AS media_distribution_emissions_avg,
    
    -- % adSelectionEmissions, creativeDistributionEmissions, mediaDistributionEmissions
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
    
    -- Performance metrics
    ROUND(SUM(total_emissions) / COUNT(*), 6) AS emissions_per_impression,
    ROUND(SUM(total_emissions) / COUNT(DISTINCT country_code), 6) AS emissions_per_country,
    
    -- Data quality insights
    ROUND(AVG(data_quality_score), 2) AS avg_data_quality_score,
    ROUND(
        (COUNT(*) - SUM(CASE WHEN has_null_emissions THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 
        2
    ) AS data_completeness_pct,
    
    -- Domain coverage analysis
    COUNT(DISTINCT domain_coverage) AS unique_domain_coverage_values,
    MODE(domain_coverage) AS most_common_domain_coverage,
    
    -- Statistical measures
    MIN(total_emissions) AS min_emissions,
    MAX(total_emissions) AS max_emissions,
    ROUND(STDDEV(total_emissions), 6) AS emissions_stddev,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_emissions) AS median_emissions,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_emissions) AS q1_emissions,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_emissions) AS q3_emissions,
    
    -- Time-based analysis
    year_month,
    COUNT(DISTINCT event_date) AS days_active_in_month,
    
    -- Business insights
    CASE 
        WHEN COUNT(*) >= 100 THEN 'High Volume'
        WHEN COUNT(*) >= 50 THEN 'Medium Volume'
        ELSE 'Low Volume'
    END AS volume_category,
    
    CASE 
        WHEN SUM(total_emissions) >= 1.0 THEN 'High Emissions'
        WHEN SUM(total_emissions) >= 0.5 THEN 'Medium Emissions'
        ELSE 'Low Emissions'
    END AS emissions_category,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS aggregated_at

FROM {{ ref('stg_advertising_emissions') }}

GROUP BY 
    event_date,
    domain,
    ad_format,
    year_month

ORDER BY 
    event_date DESC,
    total_emissions_sum DESC,
    domain,
    ad_format 