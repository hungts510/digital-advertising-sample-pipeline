-- Test that emission components approximately sum to total emissions
-- Allows for small rounding differences (within 0.001 kg)

SELECT 
    event_date,
    domain,
    ad_format,
    country_code,
    device_type,
    ad_selection_emissions,
    creative_distribution_emissions,
    media_distribution_emissions,
    total_emissions,
    calculated_total_emissions,
    ABS(total_emissions - calculated_total_emissions) AS variance
FROM {{ ref('stg_advertising_emissions') }}
WHERE 
    ABS(total_emissions - calculated_total_emissions) > 0.001  -- More than 1 gram difference
    AND has_emissions_mismatch = FALSE  -- Don't double-count flagged records 