{{
  config(
    materialized='table',
    description='Average domain coverage analysis by advertising format with comprehensive metrics'
  )
}}

SELECT
    ad_format,
    
    -- Core coverage metrics
    COUNT(*) AS total_ads,
    COUNT(DISTINCT domain) AS unique_domains,
    COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) AS ads_with_known_coverage,
    
    -- Coverage analysis (treating coverage as categorical for now)
    COUNT(DISTINCT domain_coverage) AS unique_coverage_types,
    MODE(domain_coverage) AS most_common_coverage,
    
    -- Data quality for coverage
    ROUND(
        COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) * 100.0 / COUNT(*),
        2
    ) AS coverage_data_quality_pct,
    
    -- Geographic and device distribution
    COUNT(DISTINCT country_code) AS countries_served,
    COUNT(DISTINCT device_type) AS device_types_used,
    
    -- Emissions metrics by format
    ROUND(AVG(total_emissions), 6) AS avg_emissions_per_ad,
    ROUND(SUM(total_emissions), 6) AS total_emissions,
    ROUND(SUM(total_emissions) / COUNT(DISTINCT domain), 6) AS avg_emissions_per_domain,
    
    -- Volume and activity metrics
    COUNT(DISTINCT event_date) AS active_days,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT event_date), 2) AS avg_daily_volume,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT domain), 2) AS avg_ads_per_domain,
    
    -- Market share analysis
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(),
        2
    ) AS market_share_by_volume_pct,
    
    ROUND(
        SUM(total_emissions) * 100.0 / SUM(SUM(total_emissions)) OVER(),
        2
    ) AS market_share_by_emissions_pct,
    
    -- Performance benchmarks
    CASE 
        WHEN COUNT(*) >= 300 THEN 'High Volume Format'
        WHEN COUNT(*) >= 150 THEN 'Medium Volume Format'
        WHEN COUNT(*) >= 50 THEN 'Low Volume Format'
        ELSE 'Niche Format'
    END AS volume_tier,
    
    CASE 
        WHEN ROUND(AVG(total_emissions), 6) >= 0.025 THEN 'High Emission Format'
        WHEN ROUND(AVG(total_emissions), 6) >= 0.015 THEN 'Medium Emission Format'
        ELSE 'Low Emission Format'
    END AS emission_tier,
    
    -- Coverage insights
    CASE 
        WHEN ROUND(COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) >= 90 THEN 'Excellent Coverage Data'
        WHEN ROUND(COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) >= 75 THEN 'Good Coverage Data'
        WHEN ROUND(COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) >= 50 THEN 'Fair Coverage Data'
        ELSE 'Poor Coverage Data'
    END AS coverage_quality_assessment,
    
    -- Business insights
    ROUND(
        SUM(total_emissions) / COUNT(DISTINCT country_code),
        6
    ) AS emissions_efficiency_per_country,
    
    ROUND(
        COUNT(*) * 1.0 / COUNT(DISTINCT device_type),
        2
    ) AS device_distribution_efficiency,
    
    -- Data quality metrics
    ROUND(AVG(data_quality_score), 2) AS avg_data_quality_score,
    SUM(CASE WHEN has_null_emissions THEN 1 ELSE 0 END) AS records_with_null_emissions,
    SUM(CASE WHEN has_emissions_mismatch THEN 1 ELSE 0 END) AS records_with_mismatch,
    
    -- Statistical measures
    ROUND(STDDEV(total_emissions), 6) AS emissions_stddev,
    MIN(total_emissions) AS min_emissions,
    MAX(total_emissions) AS max_emissions,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_emissions) AS median_emissions,
    
    -- Time span analysis
    MIN(event_date) AS first_activity_date,
    MAX(event_date) AS last_activity_date,
    
    -- Optimization recommendations
    CASE 
        WHEN ROUND(AVG(total_emissions), 6) >= 0.025 AND COUNT(*) >= 150 THEN 'High priority for emission optimization'
        WHEN ROUND(AVG(total_emissions), 6) >= 0.020 OR COUNT(*) >= 200 THEN 'Medium priority for optimization'
        WHEN COUNT(*) >= 100 THEN 'Monitor for growth and optimization opportunities'
        ELSE 'Low priority - focus on data quality'
    END AS optimization_priority,
    
    -- Domain coverage recommendations
    CASE 
        WHEN ROUND(COUNT(CASE WHEN domain_coverage != 'unknown' THEN 1 END) * 100.0 / COUNT(*), 2) < 50 THEN 'Improve domain coverage tracking'
        WHEN COUNT(DISTINCT domain_coverage) <= 2 THEN 'Enhance coverage categorization'
        ELSE 'Maintain current coverage tracking'
    END AS coverage_improvement_recommendation,
    
    CURRENT_TIMESTAMP() AS calculated_at

FROM {{ ref('stg_advertising_emissions') }}

GROUP BY ad_format

ORDER BY total_emissions DESC, total_ads DESC 