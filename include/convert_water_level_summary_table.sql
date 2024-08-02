--mart_data.water_level_summary
CREATE OR REPLACE VIEW mart_data.water_level_summary AS
(
    SELECT 
        wld.obs_date,
        wli.rel_river,
        SUM(CASE 
                WHEN wld.water_level >= wli.danger_level THEN 1 
                WHEN wld.water_level >= wli.warn_level THEN 1 
                WHEN wld.water_level >= wli.attn_level THEN 1 
                ELSE 0 
            END) AS attn_count,
        SUM(CASE 
                WHEN wld.water_level >= wli.danger_level THEN 1 
                WHEN wld.water_level >= wli.warn_level THEN 1 
                ELSE 0 
            END) AS warn_count,
        SUM(CASE 
                WHEN wld.water_level >= wli.danger_level THEN 1 
                ELSE 0 
            END) AS danger_count
    FROM 
        raw_data.water_level_data AS wld
    INNER JOIN 
        raw_data.water_level_info AS wli ON wld.obs_id = wli.obs_id
    WHERE 
        wld.water_level IS NOT NULL
    GROUP BY 
        wld.obs_date,
        wli.rel_river
    HAVING attn_count > 0 or warn_count > 0 or danger_count > 0
    ORDER BY 
        wld.obs_date DESC,
        wli.rel_river
);

-- mart_data.water_level_detail
CREATE OR REPLACE VIEW mart_data.water_level_detail AS
(
    SELECT 
        wld.obs_date,
        wli.obs_id,
        wli.obs_name,
        wli.rel_river,
        wli.lat,
        wli.lon,
        wld.water_level,
        CASE
            WHEN wld.water_level <= wli.attn_level THEN 0.2 * (wld.water_level / wli.attn_level)
            WHEN wld.water_level <= wli.warn_level THEN 
                0.2 + (0.5 - 0.2) * ((wld.water_level - wli.attn_level) / (wli.warn_level - wli.attn_level))
            WHEN wld.water_level <= wli.danger_level THEN 
                0.5 + (0.8 - 0.5) * ((wld.water_level - wli.warn_level) / (wli.danger_level - wli.warn_level))
            ELSE 0.8 + (1.0 - 0.8) * ((wld.water_level - wli.danger_level) / (wli.danger_level))
        END AS level_ratio
    FROM 
        raw_data.water_level_data wld
    JOIN 
        raw_data.water_level_info wli ON wld.obs_id = wli.obs_id
    WHERE 
        wld.water_level IS NOT NULL
        AND wli.attn_level IS NOT NULL
        AND wli.warn_level IS NOT NULL
        AND wli.danger_level IS NOT NULL
    ORDER BY 
        wld.obs_date DESC,
        wli.rel_river
);
