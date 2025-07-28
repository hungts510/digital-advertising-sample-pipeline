{{
  config(
    materialized='table',
    description='Top 10 domains ranked by total emissions with comprehensive metrics'
  )
}}

WITH domain_aggregates AS (
    SELECT
        domain,
        
        -- Core emissions metrics
        ROUND(SUM(total_emissions), 6) AS total_emissions,
        COUNT(*) AS total_ad_impressions,
        ROUND(AVG(total_emissions), 6) AS avg_emissions_per_ad,
        
        -- Breakdown by emission type
        ROUND(SUM(ad_selection_emissions), 6) AS total_ad_selection_emissions,
        ROUND(SUM(creative_distribution_emissions), 6) AS total_creative_distribution_emissions,
        ROUND(SUM(media_distribution_emissions), 6) AS total_media_distribution_emissions,
        
        -- Geographic and format diversity
        COUNT(DISTINCT country_code) AS countries_served,
        COUNT(DISTINCT ad_format) AS formats_used,
        COUNT(DISTINCT device_type) AS device_types_targeted,
        
        -- Performance metrics
        ROUND(SUM(total_emissions) / COUNT(*), 6) AS emissions_efficiency,
        ROUND(SUM(total_emissions) / COUNT(DISTINCT country_code), 6) AS emissions_per_country,
        
        -- Data quality
        ROUND(AVG(data_quality_score), 2) AS avg_data_quality_score,
        SUM(CASE WHEN has_null_emissions THEN 1 ELSE 0 END) AS records_with_issues,
        
        -- Statistical measures
        ROUND(STDDEV(total_emissions), 6) AS emissions_stddev,
        MIN(total_emissions) AS min_emissions,
        MAX(total_emissions) AS max_emissions,
        
        -- Domain coverage insights
        COUNT(DISTINCT domain_coverage) AS unique_coverage_types,
        MODE(domain_coverage) AS primary_coverage_type,
        
        -- Time span
        MIN(event_date) AS first_activity_date,
        MAX(event_date) AS last_activity_date,
        COUNT(DISTINCT event_date) AS active_days,
        
        -- Business insights
        CASE 
            WHEN COUNT(*) >= 100 THEN 'Enterprise'
            WHEN COUNT(*) >= 50 THEN 'Growth'
            WHEN COUNT(*) >= 20 THEN 'Emerging'
            ELSE 'Startup'
        END AS business_tier,
        
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM {{ ref('stg_advertising_emissions') }}
    GROUP BY domain
),

ranked_domains AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (ORDER BY total_emissions DESC) AS emission_rank,
        
        -- Percentage of total market emissions
        ROUND(
            total_emissions * 100.0 / SUM(total_emissions) OVER(), 
            2
        ) AS market_share_pct,
        
        -- Emission type percentages
        CASE 
            WHEN total_emissions != 0 THEN 
                ROUND((total_ad_selection_emissions / total_emissions) * 100, 2)
            ELSE 0 
        END AS ad_selection_pct,
        
        CASE 
            WHEN total_emissions != 0 THEN 
                ROUND((total_creative_distribution_emissions / total_emissions) * 100, 2)
            ELSE 0 
        END AS creative_distribution_pct,
        
        CASE 
            WHEN total_emissions != 0 THEN 
                ROUND((total_media_distribution_emissions / total_emissions) * 100, 2)
            ELSE 0 
        END AS media_distribution_pct,
        
        -- Performance benchmarks
        CASE 
            WHEN emissions_efficiency > (SELECT AVG(emissions_efficiency) FROM domain_aggregates) THEN 'Above Average'
            WHEN emissions_efficiency = (SELECT AVG(emissions_efficiency) FROM domain_aggregates) THEN 'Average'
            ELSE 'Below Average'
        END AS efficiency_benchmark,
        
        -- Growth indicators
        ROUND(
            total_ad_impressions * 1.0 / NULLIF(active_days, 0), 
            2
        ) AS avg_daily_impressions,
        
        -- Diversity score (normalized to 0-100)
        ROUND(
            ((countries_served - 1) * 20 + 
             (formats_used - 1) * 20 + 
             (device_types_targeted - 1) * 30 + 
             LEAST(active_days / 30.0 * 30, 30)), 
            1
        ) AS diversity_score

    FROM domain_aggregates
)

SELECT 
    emission_rank AS rank,
    domain,
    total_emissions,
    total_ad_impressions,
    avg_emissions_per_ad,
    countries_served,
    formats_used,
    device_types_targeted,
    market_share_pct,
    
    -- Detailed emissions breakdown
    total_ad_selection_emissions,
    total_creative_distribution_emissions,
    total_media_distribution_emissions,
    ad_selection_pct,
    creative_distribution_pct,
    media_distribution_pct,
    
    -- Performance and quality metrics
    emissions_efficiency,
    emissions_per_country,
    avg_data_quality_score,
    efficiency_benchmark,
    
    -- Business insights
    business_tier,
    diversity_score,
    avg_daily_impressions,
    active_days,
    first_activity_date,
    last_activity_date,
    
    -- Data quality
    records_with_issues,
    unique_coverage_types,
    primary_coverage_type,
    
    -- Statistical measures
    emissions_stddev,
    min_emissions,
    max_emissions,
    
    calculated_at

FROM ranked_domains

WHERE emission_rank <= 10

ORDER BY emission_rank 