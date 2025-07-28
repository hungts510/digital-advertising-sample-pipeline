{{
  config(
    materialized='table',
    description='Percentage contribution analysis of each emission type to total emissions'
  )
}}

WITH emission_totals AS (
    SELECT 
        SUM(ad_selection_emissions) AS total_ad_selection,
        SUM(creative_distribution_emissions) AS total_creative_distribution,
        SUM(media_distribution_emissions) AS total_media_distribution,
        SUM(total_emissions) AS grand_total_emissions,
        COUNT(*) AS total_records,
        COUNT(DISTINCT domain) AS total_domains
    FROM {{ ref('stg_advertising_emissions') }}
),

emission_contributions AS (
    SELECT 
        'adSelectionEmissions' AS emission_type,
        'Ad Selection Process' AS emission_description,
        (SELECT total_ad_selection FROM emission_totals) AS total_value,
        ROUND(
            (SELECT total_ad_selection FROM emission_totals) * 100.0 / 
            NULLIF((SELECT grand_total_emissions FROM emission_totals), 0),
            2
        ) AS percentage_contribution,
        'Primary driver of emissions in advertising technology stack' AS business_context,
        CASE 
            WHEN (SELECT total_ad_selection FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 80 THEN 'Dominant'
            WHEN (SELECT total_ad_selection FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 50 THEN 'Major'
            WHEN (SELECT total_ad_selection FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 20 THEN 'Moderate'
            ELSE 'Minor'
        END AS impact_level,
        1 AS sort_order
        
    UNION ALL
    
    SELECT 
        'creativeDistributionEmissions' AS emission_type,
        'Creative Distribution Process' AS emission_description,
        (SELECT total_creative_distribution FROM emission_totals) AS total_value,
        ROUND(
            (SELECT total_creative_distribution FROM emission_totals) * 100.0 / 
            NULLIF((SELECT grand_total_emissions FROM emission_totals), 0),
            2
        ) AS percentage_contribution,
        'Emissions from distributing advertising creative assets' AS business_context,
        CASE 
            WHEN (SELECT total_creative_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 80 THEN 'Dominant'
            WHEN (SELECT total_creative_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 50 THEN 'Major'
            WHEN (SELECT total_creative_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 20 THEN 'Moderate'
            ELSE 'Minor'
        END AS impact_level,
        2 AS sort_order
        
    UNION ALL
    
    SELECT 
        'mediaDistributionEmissions' AS emission_type,
        'Media Distribution Process' AS emission_description,
        (SELECT total_media_distribution FROM emission_totals) AS total_value,
        ROUND(
            (SELECT total_media_distribution FROM emission_totals) * 100.0 / 
            NULLIF((SELECT grand_total_emissions FROM emission_totals), 0),
            2
        ) AS percentage_contribution,
        'Emissions from media delivery and serving infrastructure' AS business_context,
        CASE 
            WHEN (SELECT total_media_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 80 THEN 'Dominant'
            WHEN (SELECT total_media_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 50 THEN 'Major'
            WHEN (SELECT total_media_distribution FROM emission_totals) * 100.0 / 
                 NULLIF((SELECT grand_total_emissions FROM emission_totals), 0) > 20 THEN 'Moderate'
            ELSE 'Minor'
        END AS impact_level,
        3 AS sort_order
),

validation_checks AS (
    SELECT 
        SUM(percentage_contribution) AS total_percentage,
        SUM(total_value) AS sum_of_components,
        (SELECT grand_total_emissions FROM emission_totals) AS expected_total,
        ABS(SUM(total_value) - (SELECT grand_total_emissions FROM emission_totals)) AS variance,
        CASE 
            WHEN ABS(SUM(total_value) - (SELECT grand_total_emissions FROM emission_totals)) < 0.01 THEN 'PASS'
            WHEN ABS(SUM(total_value) - (SELECT grand_total_emissions FROM emission_totals)) < 0.1 THEN 'WARNING'
            ELSE 'FAIL'
        END AS validation_status
    FROM emission_contributions
),

enriched_contributions AS (
    SELECT 
        ec.*,
        
        -- Add rank by contribution
        ROW_NUMBER() OVER (ORDER BY percentage_contribution DESC) AS contribution_rank,
        
        -- Add cumulative percentage
        SUM(percentage_contribution) OVER (
            ORDER BY percentage_contribution DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_percentage,
        
        -- Add context metrics
        ROUND(total_value / (SELECT total_records FROM emission_totals), 6) AS avg_per_record,
        ROUND(total_value / (SELECT total_domains FROM emission_totals), 6) AS avg_per_domain,
        
        -- Add optimization potential
        CASE 
            WHEN percentage_contribution > 80 THEN 'High optimization potential - primary focus area'
            WHEN percentage_contribution > 50 THEN 'Medium optimization potential - secondary focus'
            WHEN percentage_contribution > 20 THEN 'Lower optimization potential - monitor'
            ELSE 'Minimal optimization potential - maintenance mode'
        END AS optimization_recommendation,
        
        -- Add efficiency metrics
        CASE 
            WHEN emission_type = 'adSelectionEmissions' THEN 'Algorithm optimization, caching, reduced computational complexity'
            WHEN emission_type = 'creativeDistributionEmissions' THEN 'CDN optimization, compression, edge caching'
            WHEN emission_type = 'mediaDistributionEmissions' THEN 'Bandwidth optimization, efficient encoding, smart routing'
            ELSE 'General efficiency improvements'
        END AS efficiency_strategies,
        
        -- Add validation context
        (SELECT validation_status FROM validation_checks) AS data_validation_status,
        (SELECT total_percentage FROM validation_checks) AS total_percentage_check,
        
        -- Metadata
        (SELECT total_records FROM emission_totals) AS based_on_records,
        (SELECT total_domains FROM emission_totals) AS based_on_domains,
        CURRENT_TIMESTAMP() AS calculated_at
        
    FROM emission_contributions ec
)

SELECT 
    emission_type,
    emission_description,
    total_value,
    percentage_contribution,
    contribution_rank,
    cumulative_percentage,
    impact_level,
    business_context,
    
    -- Performance metrics
    avg_per_record,
    avg_per_domain,
    
    -- Business insights
    optimization_recommendation,
    efficiency_strategies,
    
    -- Data validation
    data_validation_status,
    total_percentage_check,
    
    -- Context
    based_on_records,
    based_on_domains,
    calculated_at

FROM enriched_contributions

ORDER BY sort_order 