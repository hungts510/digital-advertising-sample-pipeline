{{ config(
    materialized='table',
    schema='business_logic'
) }}

-- Business Logic Feature 2: Identify data quality issues and their impact
-- Comprehensive analysis of data quality patterns and business impact

WITH base_stats AS (
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT domain) AS total_domains,
        COUNT(DISTINCT date) AS total_days,
        ROUND(SUM(total_emissions), 3) AS total_emissions_all,
        ROUND(AVG(data_quality_score), 1) AS overall_quality_score
    FROM {{ ref('stg_advertising_emissions') }}
),

quality_issues AS (
    -- Null emissions analysis
    SELECT 
        'null_emissions' AS issue_type,
        'Null Emissions Values' AS issue_description,
        COUNT(*) AS affected_records,
        COUNT(DISTINCT domain) AS affected_domains,
        ROUND(COUNT(*) * 100.0 / (SELECT total_records FROM base_stats), 2) AS records_impact_pct,
        ROUND(COUNT(DISTINCT domain) * 100.0 / (SELECT total_domains FROM base_stats), 2) AS domains_impact_pct,
        0.0 AS emissions_impact,
        'HIGH' AS severity_level,
        'Data Completeness' AS category,
        CURRENT_TIMESTAMP() AS analyzed_at
    FROM {{ ref('stg_advertising_emissions') }}
    WHERE has_null_emissions = TRUE
    
    UNION ALL
    
    -- Emissions mismatch analysis
    SELECT 
        'emissions_mismatch' AS issue_type,
        'Component Emissions Do Not Sum to Total' AS issue_description,
        COUNT(*) AS affected_records,
        COUNT(DISTINCT domain) AS affected_domains,
        ROUND(COUNT(*) * 100.0 / (SELECT total_records FROM base_stats), 2) AS records_impact_pct,
        ROUND(COUNT(DISTINCT domain) * 100.0 / (SELECT total_domains FROM base_stats), 2) AS domains_impact_pct,
        ROUND(SUM(ABS(total_emissions - (ad_selection_emissions + creative_distribution_emissions + media_distribution_emissions))), 3) AS emissions_impact,
        'MEDIUM' AS severity_level,
        'Data Accuracy' AS category,
        CURRENT_TIMESTAMP() AS analyzed_at
    FROM {{ ref('stg_advertising_emissions') }}
    WHERE has_emissions_mismatch = TRUE
    
    UNION ALL
    
    -- Extreme emissions analysis
    SELECT 
        'extreme_emissions' AS issue_type,
        'Emissions Values Outside Expected Range' AS issue_description,
        COUNT(*) AS affected_records,
        COUNT(DISTINCT domain) AS affected_domains,
        ROUND(COUNT(*) * 100.0 / (SELECT total_records FROM base_stats), 2) AS records_impact_pct,
        ROUND(COUNT(DISTINCT domain) * 100.0 / (SELECT total_domains FROM base_stats), 2) AS domains_impact_pct,
        ROUND(SUM(ABS(total_emissions)), 3) AS emissions_impact,
        CASE 
            WHEN COUNT(*) > (SELECT total_records FROM base_stats) * 0.05 THEN 'HIGH'
            WHEN COUNT(*) > (SELECT total_records FROM base_stats) * 0.01 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS severity_level,
        'Data Validity' AS category,
        CURRENT_TIMESTAMP() AS analyzed_at
    FROM {{ ref('stg_advertising_emissions') }}
    WHERE has_extreme_emissions = TRUE
    
    UNION ALL
    
    -- Low quality score analysis
    SELECT 
        'low_quality_score' AS issue_type,
        'Records with Data Quality Score Below 70' AS issue_description,
        COUNT(*) AS affected_records,
        COUNT(DISTINCT domain) AS affected_domains,
        ROUND(COUNT(*) * 100.0 / (SELECT total_records FROM base_stats), 2) AS records_impact_pct,
        ROUND(COUNT(DISTINCT domain) * 100.0 / (SELECT total_domains FROM base_stats), 2) AS domains_impact_pct,
        ROUND(SUM(total_emissions), 3) AS emissions_impact,
        'MEDIUM' AS severity_level,
        'Overall Quality' AS category,
        CURRENT_TIMESTAMP() AS analyzed_at
    FROM {{ ref('stg_advertising_emissions') }}
    WHERE data_quality_score < 70
),

quality_summary AS (
    SELECT 
        -- Issue summary
        COUNT(*) AS total_issue_types,
        SUM(affected_records) AS total_affected_records,
        SUM(affected_domains) AS total_affected_domains,
        ROUND(AVG(records_impact_pct), 2) AS avg_records_impact_pct,
        
        -- Emissions impact
        ROUND(SUM(emissions_impact), 3) AS total_emissions_at_risk,
        
        -- Severity breakdown
        SUM(CASE WHEN severity_level = 'HIGH' THEN affected_records ELSE 0 END) AS high_severity_records,
        SUM(CASE WHEN severity_level = 'MEDIUM' THEN affected_records ELSE 0 END) AS medium_severity_records,
        SUM(CASE WHEN severity_level = 'LOW' THEN affected_records ELSE 0 END) AS low_severity_records,
        
        CURRENT_TIMESTAMP() AS summary_generated_at
    FROM quality_issues
    WHERE affected_records > 0
),

recommendations AS (
    SELECT 
        issue_type,
        issue_description,
        severity_level,
        affected_records,
        records_impact_pct,
        
        -- Generate specific recommendations
        CASE 
            WHEN issue_type = 'null_emissions' THEN 'Implement data validation at ingestion. Review data source quality.'
            WHEN issue_type = 'emissions_mismatch' THEN 'Verify calculation logic. Check for rounding errors in source systems.'
            WHEN issue_type = 'extreme_emissions' THEN 'Implement outlier detection. Review business rules for valid ranges.'
            WHEN issue_type = 'low_quality_score' THEN 'Improve data collection processes. Implement quality gates.'
            ELSE 'Review data quality processes'
        END AS recommendation,
        
        -- Priority based on impact and severity
        CASE 
            WHEN severity_level = 'HIGH' AND records_impact_pct > 5 THEN 'URGENT'
            WHEN severity_level = 'HIGH' OR records_impact_pct > 10 THEN 'HIGH'
            WHEN severity_level = 'MEDIUM' OR records_impact_pct > 5 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS priority
        
    FROM quality_issues
    WHERE affected_records > 0
)

-- Final output combining all analyses
SELECT 
    qi.*,
    r.recommendation,
    r.priority,
    
    -- Add context from summary
    (SELECT overall_quality_score FROM base_stats) AS baseline_quality_score,
    (SELECT total_records FROM base_stats) AS total_records_context,
    
    -- Business impact assessment
    CASE 
        WHEN qi.records_impact_pct > 10 AND qi.severity_level = 'HIGH' THEN 'Critical business impact'
        WHEN qi.records_impact_pct > 5 OR qi.severity_level = 'HIGH' THEN 'Significant business impact'
        WHEN qi.records_impact_pct > 1 OR qi.severity_level = 'MEDIUM' THEN 'Moderate business impact'
        ELSE 'Minor business impact'
    END AS business_impact_assessment

FROM quality_issues qi
LEFT JOIN recommendations r ON qi.issue_type = r.issue_type

WHERE qi.affected_records > 0

ORDER BY 
    CASE qi.severity_level 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        WHEN 'LOW' THEN 3 
    END,
    qi.records_impact_pct DESC 