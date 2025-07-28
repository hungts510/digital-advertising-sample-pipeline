-- Data Quality Helper Macros

{% macro calculate_data_quality_score(
    has_null_emissions,
    has_emissions_mismatch,
    has_extreme_emissions,
    domain_is_null,
    country_is_null
) %}
    100 - (
        CASE WHEN {{ has_null_emissions }} THEN 30 ELSE 0 END +
        CASE WHEN {{ has_emissions_mismatch }} THEN 20 ELSE 0 END +
        CASE WHEN {{ has_extreme_emissions }} THEN 25 ELSE 0 END +
        CASE WHEN {{ domain_is_null }} THEN 15 ELSE 0 END +
        CASE WHEN {{ country_is_null }} THEN 10 ELSE 0 END
    )
{% endmacro %}

{% macro safe_percentage(numerator, denominator) %}
    CASE 
        WHEN {{ denominator }} != 0 THEN 
            ROUND(({{ numerator }} / {{ denominator }}) * 100, 2)
        ELSE 0 
    END
{% endmacro %}

{% macro emission_variance_check(total_emissions, calculated_total, tolerance=0.001) %}
    ABS({{ total_emissions }} - {{ calculated_total }}) > {{ tolerance }}
{% endmacro %}

{% macro categorize_by_volume(record_count, high_threshold=100, medium_threshold=50) %}
    CASE 
        WHEN {{ record_count }} >= {{ high_threshold }} THEN 'High Volume'
        WHEN {{ record_count }} >= {{ medium_threshold }} THEN 'Medium Volume'
        ELSE 'Low Volume'
    END
{% endmacro %}

{% macro categorize_by_emissions(avg_emissions, high_threshold=0.025, medium_threshold=0.015) %}
    CASE 
        WHEN {{ avg_emissions }} >= {{ high_threshold }} THEN 'High Emissions'
        WHEN {{ avg_emissions }} >= {{ medium_threshold }} THEN 'Medium Emissions'
        ELSE 'Low Emissions'
    END
{% endmacro %}

{% macro business_tier_classification(record_count) %}
    CASE 
        WHEN {{ record_count }} >= 100 THEN 'Enterprise'
        WHEN {{ record_count }} >= 50 THEN 'Growth'
        WHEN {{ record_count }} >= 20 THEN 'Emerging'
        ELSE 'Startup'
    END
{% endmacro %}

{% macro severity_level(impact_pct, high_threshold=5.0, medium_threshold=1.0) %}
    CASE 
        WHEN {{ impact_pct }} > {{ high_threshold }} THEN 'HIGH'
        WHEN {{ impact_pct }} > {{ medium_threshold }} THEN 'MEDIUM'
        ELSE 'LOW'
    END
{% endmacro %}

{% macro get_date_components(date_column) %}
    YEAR({{ date_column }}) AS year,
    MONTH({{ date_column }}) AS month,
    DAY({{ date_column }}) AS day,
    DAYOFWEEK({{ date_column }}) AS day_of_week,
    DATE_FORMAT({{ date_column }}, 'yyyy-MM') AS year_month
{% endmacro %} 