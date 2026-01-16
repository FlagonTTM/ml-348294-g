-- Объекты витрины рекомендаций для GE в схеме sandbox

-- 1. Основная таблица фичей (структура совпадает с прежней city_position_features)
CREATE TABLE IF NOT EXISTS sandbox.city_position_features_ge (
    week_dt DATE NOT NULL,
    position_id INT NOT NULL,
    location_rk TEXT NOT NULL,
    location_nm TEXT,
    position_name TEXT,
    ext_salary_mean NUMERIC(14,2),
    ext_salary_p25 NUMERIC(14,2),
    ext_salary_p50 NUMERIC(14,2),
    ext_salary_p75 NUMERIC(14,2),
    ext_vacancy_cnt INT,
    ext_vacancy_hrm INT,
    ext_vacancy_sal_range NUMERIC(14,2),
    dismissal_int_reason_group_nm TEXT,
    dismissal_cnt INT,
    turnover_coeff NUMERIC(18,5),
    required_positions INT,
    occupied_positions INT,
    normalized_positions INT,
    headcount_gap INT,
    average_inhouse_headcount_coeff NUMERIC(18,5),
    average_headcount_coeff NUMERIC(18,5),
    int_salary_tc5 NUMERIC(14,2),
    target_salary_amt NUMERIC,
    competitor_nm TEXT,
    competitor_type TEXT,
    competitor_food_cat TEXT CHECK (competitor_food_cat IN ('Food', 'Non-food', 'Не определено')),
    post_cnt INT,
    agglomeration_nm TEXT,
    gap_to_market_p50 NUMERIC(18,5),
    gap_to_market_p25 NUMERIC(18,5),
    lag1_ext_salary_p50 NUMERIC,
    lag1_ext_salary_p25 NUMERIC,
    lag1_turnover NUMERIC,
    rolling_mean_turnover_4w NUMERIC,
    lag1_post_cnt NUMERIC,
    rolling_mean_post_4w NUMERIC,
    n_weeks_12w INT,
    avg_vacancy_12w INT,
    avg_post_cnt_12w INT,
    lag52_ext_salary_p50 NUMERIC,
    salary_p50_yoy_diff NUMERIC,
    avg_ext_vacancy_w52 INT,
    avg_post_cnt_52w INT,
    vacancy_cnt_yoy_diff NUMERIC,
    lag52_turnover_coeff NUMERIC,
    turnover_yoy_diff NUMERIC,
    lag52_gap_to_market_p50 NUMERIC,
    lag52_gap_to_market_p25 NUMERIC,
    gap_yoy_diff NUMERIC,
    nulls_ratio NUMERIC,
    data_insufficient BOOLEAN,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_city_position_features_ge PRIMARY KEY (week_dt, position_id, location_rk)
);

-- 2. Таблица флагов проблем (структура как sandbox.hr_problem_flags)
CREATE TABLE IF NOT EXISTS sandbox.hr_problem_flags_ge (
    week_dt DATE NOT NULL,
    position_id INT NOT NULL,
    location_rk TEXT NOT NULL,
    location_nm TEXT,
    position_name TEXT,
    --flag
    problem_flg BOOLEAN,
    problem_trend BOOLEAN,
    problem_normalized_positions BOOLEAN,
    iso_flag BOOLEAN,
    iso_score DOUBLE PRECISION,
    vacancy_spike_ext_flg BOOLEAN,
    salary_trend_flg BOOLEAN,
    salary_gap_neg_flg BOOLEAN,
    salary_gap_pos_flg BOOLEAN,
    vacancy_gap_int_flg BOOLEAN,
    turnover_trend_flg BOOLEAN,
    different_vacancy_ext_int BOOLEAN,
    competition_intense_flg BOOLEAN,
    --salary
    int_salary_tc5 NUMERIC,
    target_salary_amt NUMERIC,
    ext_salary_p25 NUMERIC,
    ext_salary_p50 NUMERIC,
    ext_salary_p75 NUMERIC,
    salary_delta_p50 NUMERIC,
    salary_delta_p25 NUMERIC,
    salary_range NUMERIC,
    salary_std_4w NUMERIC,
    ext_salary_p50_ema NUMERIC,
    --competitor
    competitor_nm TEXT,
    competitor_type TEXT,
    competitor_food_cat TEXT,
    --vacancy
    normalized_positions NUMERIC,
    headcount_gap_share NUMERIC,
    average_inhouse_headcount_coeff NUMERIC,
    average_headcount_coeff NUMERIC,
    required_positions NUMERIC,
    occupied_positions NUMERIC,
    vac_ext_int_ratio NUMERIC,
    --yoy
    salary_p50_yoy_diff NUMERIC,
    vacancy_cnt_yoy_diff NUMERIC,
    turnover_yoy_diff NUMERIC,
    --month and quart
    is_december BOOLEAN,
    is_january BOOLEAN,
    is_q1 BOOLEAN,
    is_q4 BOOLEAN,
    --robust
    gap_neg_robust_flg BOOLEAN,
    gap_pos_robust_flg BOOLEAN,
    headcount_gap_robust_flg BOOLEAN,
    vac_ext_int_robust_flg BOOLEAN,
    turnover_robust_flg BOOLEAN,
    created_at TIMESTAMPTZ DEFAULT now(),
    CONSTRAINT pk_hr_problem_flags_ge PRIMARY KEY (week_dt, position_id, location_rk)
);

-- 3. Таблица рекомендаций (структура как sandbox.hr_recommendations)
CREATE TABLE IF NOT EXISTS sandbox.hr_recommendations_ge (
    week_dt DATE NOT NULL,
    position_id INT NOT NULL,
    position_name TEXT,
    location_rk TEXT NOT NULL,
    location_nm TEXT,
    agglomeration_name TEXT,
    work_schedule_type TEXT,
    problem_type TEXT NOT NULL,
    severity SMALLINT NOT NULL DEFAULT 1,
    text_ru TEXT,
    meta_json JSONB,
    fact_salary_fte_amt NUMERIC(14,2),
    market_perc_50 NUMERIC(14,2),
    gap_to_market_p50 NUMERIC(18,5),
    cnt_vacancy_site INT,
    vacancy_cnt INT,
    target_salary NUMERIC(14,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_hr_recommendations_ge PRIMARY KEY (week_dt, position_id, location_rk, problem_type)
);

-- 4. Процедура обновления таблицы фичей (максимально повторяет логику прошлых рекомендаций, но использует доступные GE-источники)
CREATE OR REPLACE PROCEDURE sandbox.prc_update_city_position_features_ge()
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO stg_sys.logging (prc_name, description_step)
    VALUES ('PRC_UPDATE_CITY_POSITION_FEATURES_GE', 'Процедура запущена');

    WITH params AS (
        SELECT date_trunc('month', current_date - interval '18 months') AS cutoff_month,
               date_trunc('month', current_date) AS this_month
    ),
    base AS (
        SELECT
            date_trunc('month', i.month_dt)::date AS week_dt,
            i.position_type_id                    AS position_id,
            COALESCE(i.location_rk, '')           AS location_rk,
            i.location_nm,
            COALESCE(i.hrm_job_title_nm, pt.name) AS position_name,
            i.agglomeration_name                  AS agglomeration_nm,
            i.work_schedule_type,
            i.fact_salary_fte_amt                 AS int_salary_tc5,
            NULLIF(i.target_salary, 0)            AS target_salary_amt,
            i.cnt_emp_dist                        AS cnt_emp_dist,
            i.perc_25                             AS perc_25,
            i.perc_50                             AS perc_50,
            i.perc_75                             AS perc_75,
            i.cnt_vacancy_site                    AS market_cnt_vacancy_site,
            i.company_competitor
        FROM stg_rep.itog_for_ch_int_salary_ge i
        LEFT JOIN stg_core.vw_dim_positiontype pt ON pt.id = i.position_type_id
        WHERE i.month_dt >= (SELECT cutoff_month FROM params)
    ),
    ext AS (
        SELECT
            date_trunc('month', month_dt)::date AS month_dt,
            position_type_id,
            agglomeration_name,
            COALESCE(perc_25, perc_25_3month, perc_25_6month) AS perc_25,
            COALESCE(perc_50, perc_50_3month, perc_50_6month) AS perc_50,
            COALESCE(perc_75, perc_75_3month, perc_75_6month) AS perc_75,
            cnt_vacancy_site
        FROM stg_rep.ext_vacancies_agg
        WHERE month_dt >= (SELECT cutoff_month FROM params)
          AND type_agg = 'Весь рынок'
    ),
    vac AS (
        SELECT
            date_trunc('month', month_dt)::date AS month_dt,
            position_type_id,
            agglomeration_name,
            COUNT(*) AS vacancy_cnt
        FROM stg_rep.ext_vacancies_for_calc_agg
        WHERE month_dt >= (SELECT cutoff_month FROM params)
        GROUP BY 1, 2, 3
    ),
    joined AS (
        SELECT
            b.week_dt,
            b.position_id,
            b.location_rk,
            b.location_nm,
            b.position_name,
            b.agglomeration_nm,
            b.work_schedule_type,
            b.int_salary_tc5,
            b.target_salary_amt,
            b.cnt_emp_dist,
            COALESCE(e.perc_25, b.perc_25) AS market_perc_25,
            COALESCE(e.perc_50, b.perc_50) AS market_perc_50,
            COALESCE(e.perc_75, b.perc_75) AS market_perc_75,
            COALESCE(e.cnt_vacancy_site, b.market_cnt_vacancy_site) AS cnt_vacancy_site,
            COALESCE(v.vacancy_cnt, 0) AS vacancy_cnt,
            b.company_competitor
        FROM base b
        LEFT JOIN ext e
          ON e.month_dt = b.week_dt
         AND e.position_type_id = b.position_id
         AND COALESCE(e.agglomeration_name, '') = COALESCE(b.agglomeration_nm, '')
        LEFT JOIN vac v
          ON v.month_dt = b.week_dt
         AND v.position_type_id = b.position_id
         AND COALESCE(v.agglomeration_name, '') = COALESCE(b.agglomeration_nm, '')
    ),
    lagged AS (
        SELECT
            j.*,
            LAG(j.market_perc_50) OVER w AS lag1_ext_salary_p50,
            LAG(j.market_perc_25) OVER w AS lag1_ext_salary_p25,
            COUNT(*) OVER w12 AS n_weeks_12w,
            AVG(j.cnt_vacancy_site) OVER w12 AS avg_vacancy_12w,
            LAG(j.cnt_vacancy_site) OVER w AS lag1_post_cnt,
            AVG(j.cnt_vacancy_site) OVER w3 AS rolling_mean_post_4w,
            AVG(j.cnt_vacancy_site) OVER w12 AS avg_post_cnt_12w,
            LAG(j.market_perc_50, 12) OVER w AS lag52_ext_salary_p50,
            CASE
                WHEN LAG(j.market_perc_50, 12) OVER w IS NOT NULL AND LAG(j.market_perc_50, 12) OVER w <> 0
                    THEN j.market_perc_50 / LAG(j.market_perc_50, 12) OVER w - 1
            END AS salary_p50_yoy_diff,
            AVG(j.cnt_vacancy_site) OVER w52 AS avg_ext_vacancy_w52,
            AVG(j.cnt_vacancy_site) OVER w52 AS avg_post_cnt_52w,
            CASE
                WHEN LAG(j.cnt_vacancy_site, 12) OVER w IS NOT NULL AND LAG(j.cnt_vacancy_site, 12) OVER w <> 0
                    THEN j.cnt_vacancy_site / LAG(j.cnt_vacancy_site, 12) OVER w - 1
            END AS vacancy_cnt_yoy_diff,
            CASE
                WHEN j.market_perc_50 IS NOT NULL AND j.market_perc_50 > 0 AND j.int_salary_tc5 IS NOT NULL
                    THEN (j.int_salary_tc5 - j.market_perc_50) / j.market_perc_50
            END AS gap_to_market_p50,
            CASE
                WHEN j.market_perc_25 IS NOT NULL AND j.market_perc_25 > 0 AND j.int_salary_tc5 IS NOT NULL
                    THEN (j.int_salary_tc5 - j.market_perc_25) / j.market_perc_25
            END AS gap_to_market_p25,
            LAG(
                CASE
                    WHEN j.market_perc_50 IS NOT NULL AND j.market_perc_50 > 0 AND j.int_salary_tc5 IS NOT NULL
                        THEN (j.int_salary_tc5 - j.market_perc_50) / j.market_perc_50
                END, 12
            ) OVER w AS lag52_gap_to_market_p50,
            LAG(
                CASE
                    WHEN j.market_perc_25 IS NOT NULL AND j.market_perc_25 > 0 AND j.int_salary_tc5 IS NOT NULL
                        THEN (j.int_salary_tc5 - j.market_perc_25) / j.market_perc_25
                END, 12
            ) OVER w AS lag52_gap_to_market_p25
        FROM joined j
        WINDOW
            w AS (PARTITION BY position_id, location_rk ORDER BY week_dt),
            w3 AS (PARTITION BY position_id, location_rk ORDER BY week_dt ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING),
            w12 AS (PARTITION BY position_id, location_rk ORDER BY week_dt ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
            w52 AS (PARTITION BY position_id, location_rk ORDER BY week_dt ROWS BETWEEN 51 PRECEDING AND CURRENT ROW)
    ),
    final_data AS (
        SELECT DISTINCT ON (week_dt, position_id, location_rk)
            l.week_dt,
            l.position_id,
            l.location_rk,
            l.location_nm,
            l.position_name,
            l.market_perc_50 AS ext_salary_mean,
            l.market_perc_25 AS ext_salary_p25,
            l.market_perc_50 AS ext_salary_p50,
            l.market_perc_75 AS ext_salary_p75,
            COALESCE(l.cnt_vacancy_site, l.vacancy_cnt, 0) AS ext_vacancy_cnt,
            NULL::NUMERIC(18,5) AS ext_vacancy_hrm,
            CASE
                WHEN l.market_perc_75 IS NOT NULL AND l.market_perc_25 IS NOT NULL THEN l.market_perc_75 - l.market_perc_25
            END AS ext_vacancy_sal_range,
            NULL::NUMERIC(18,5) AS turnover_coeff,
            NULL::NUMERIC(18,5) AS required_positions,
            NULL::NUMERIC(18,5) AS occupied_positions,
            CASE WHEN l.cnt_emp_dist IS NOT NULL AND l.cnt_emp_dist > 0 THEN 100 ELSE NULL END AS normalized_positions,
            NULL::NUMERIC(18,5) AS headcount_gap,
            NULL::NUMERIC(18,5) AS average_inhouse_headcount_coeff,
            NULL::NUMERIC(18,5) AS average_headcount_coeff,
            NULL::TEXT AS dismissal_int_reason_group_nm,
            NULL::NUMERIC(18,5) AS dismissal_cnt,
            l.int_salary_tc5,
            l.target_salary_amt,
            NULLIF(l.company_competitor, 'не определено') AS competitor_nm,
            NULL::TEXT AS competitor_type,
            NULL::TEXT AS competitor_food_cat,
            NULL::NUMERIC(18,5) AS post_cnt,
            l.agglomeration_nm,
            l.gap_to_market_p50,
            l.gap_to_market_p25,
            l.lag1_ext_salary_p50,
            l.lag1_ext_salary_p25,
            NULL::NUMERIC(18,5) AS lag1_turnover,
            NULL::NUMERIC(18,5) AS rolling_mean_turnover_4w,
            l.n_weeks_12w,
            l.avg_vacancy_12w,
            l.lag1_post_cnt,
            l.rolling_mean_post_4w,
            l.avg_post_cnt_12w,
            l.lag52_ext_salary_p50,
            l.salary_p50_yoy_diff,
            l.avg_ext_vacancy_w52,
            l.avg_post_cnt_52w,
            l.vacancy_cnt_yoy_diff,
            NULL::NUMERIC(18,5) AS lag52_turnover_coeff,
            NULL::NUMERIC(18,5) AS turnover_yoy_diff,
            l.lag52_gap_to_market_p50,
            l.lag52_gap_to_market_p25,
            CASE
                WHEN l.lag52_gap_to_market_p50 IS NOT NULL AND l.lag52_gap_to_market_p50 <> 0 AND l.gap_to_market_p50 IS NOT NULL
                    THEN l.gap_to_market_p50 / l.lag52_gap_to_market_p50 - 1
            END AS gap_yoy_diff,
            (
                (CASE WHEN l.market_perc_50 IS NULL THEN 1 ELSE 0 END) +
                (CASE WHEN l.int_salary_tc5 IS NULL THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(l.cnt_vacancy_site, l.vacancy_cnt, 0) = 0 THEN 1 ELSE 0 END)
            )::NUMERIC / 3 AS nulls_ratio,
            (
                (CASE WHEN l.market_perc_50 IS NULL THEN 1 ELSE 0 END) +
                (CASE WHEN l.int_salary_tc5 IS NULL THEN 1 ELSE 0 END) +
                (CASE WHEN COALESCE(l.cnt_vacancy_site, l.vacancy_cnt, 0) = 0 THEN 1 ELSE 0 END)
            )::NUMERIC / 3 > 0.3 AS data_insufficient,
            now() AS inserted_at
        FROM lagged l
        ORDER BY week_dt, position_id, location_rk
    )
    INSERT INTO sandbox.city_position_features_ge AS t (
        week_dt, position_id, location_rk, location_nm, position_name,
        ext_salary_mean, ext_salary_p25, ext_salary_p50, ext_salary_p75,
        ext_vacancy_cnt, ext_vacancy_hrm, ext_vacancy_sal_range,
        turnover_coeff, required_positions, occupied_positions, normalized_positions, headcount_gap,
        average_inhouse_headcount_coeff, average_headcount_coeff, dismissal_int_reason_group_nm, dismissal_cnt,
        int_salary_tc5, target_salary_amt, competitor_nm, competitor_type, competitor_food_cat, post_cnt,
        agglomeration_nm, gap_to_market_p50, gap_to_market_p25,
        lag1_ext_salary_p50, lag1_ext_salary_p25, lag1_turnover, rolling_mean_turnover_4w,
        n_weeks_12w, avg_vacancy_12w, lag1_post_cnt, rolling_mean_post_4w, avg_post_cnt_12w,
        lag52_ext_salary_p50, salary_p50_yoy_diff,
        avg_ext_vacancy_w52, avg_post_cnt_52w, vacancy_cnt_yoy_diff,
        lag52_turnover_coeff, turnover_yoy_diff,
        lag52_gap_to_market_p50, lag52_gap_to_market_p25, gap_yoy_diff,
        nulls_ratio, data_insufficient, inserted_at
    )
    SELECT
        week_dt, position_id, location_rk, location_nm, position_name,
        ext_salary_mean, ext_salary_p25, ext_salary_p50, ext_salary_p75,
        ext_vacancy_cnt, ext_vacancy_hrm, ext_vacancy_sal_range,
        turnover_coeff, required_positions, occupied_positions, normalized_positions, headcount_gap,
        average_inhouse_headcount_coeff, average_headcount_coeff, dismissal_int_reason_group_nm, dismissal_cnt,
        int_salary_tc5, target_salary_amt, competitor_nm, competitor_type, competitor_food_cat, post_cnt,
        agglomeration_nm, gap_to_market_p50, gap_to_market_p25,
        lag1_ext_salary_p50, lag1_ext_salary_p25, lag1_turnover, rolling_mean_turnover_4w,
        n_weeks_12w, avg_vacancy_12w, lag1_post_cnt, rolling_mean_post_4w, avg_post_cnt_12w,
        lag52_ext_salary_p50, salary_p50_yoy_diff,
        avg_ext_vacancy_w52, avg_post_cnt_52w, vacancy_cnt_yoy_diff,
        lag52_turnover_coeff, turnover_yoy_diff,
        lag52_gap_to_market_p50, lag52_gap_to_market_p25, gap_yoy_diff,
        nulls_ratio, data_insufficient, inserted_at
    FROM final_data
    ON CONFLICT (week_dt, position_id, location_rk) DO UPDATE SET
        location_nm = EXCLUDED.location_nm,
        position_name = EXCLUDED.position_name,
        ext_salary_mean = EXCLUDED.ext_salary_mean,
        ext_salary_p25 = EXCLUDED.ext_salary_p25,
        ext_salary_p50 = EXCLUDED.ext_salary_p50,
        ext_salary_p75 = EXCLUDED.ext_salary_p75,
        ext_vacancy_cnt = EXCLUDED.ext_vacancy_cnt,
        ext_vacancy_hrm = EXCLUDED.ext_vacancy_hrm,
        ext_vacancy_sal_range = EXCLUDED.ext_vacancy_sal_range,
        turnover_coeff = EXCLUDED.turnover_coeff,
        required_positions = EXCLUDED.required_positions,
        occupied_positions = EXCLUDED.occupied_positions,
        normalized_positions = EXCLUDED.normalized_positions,
        headcount_gap = EXCLUDED.headcount_gap,
        average_inhouse_headcount_coeff = EXCLUDED.average_inhouse_headcount_coeff,
        average_headcount_coeff = EXCLUDED.average_headcount_coeff,
        dismissal_int_reason_group_nm = EXCLUDED.dismissal_int_reason_group_nm,
        dismissal_cnt = EXCLUDED.dismissal_cnt,
        int_salary_tc5 = EXCLUDED.int_salary_tc5,
        target_salary_amt = EXCLUDED.target_salary_amt,
        competitor_nm = EXCLUDED.competitor_nm,
        competitor_type = EXCLUDED.competitor_type,
        competitor_food_cat = EXCLUDED.competitor_food_cat,
        post_cnt = EXCLUDED.post_cnt,
        agglomeration_nm = EXCLUDED.agglomeration_nm,
        gap_to_market_p50 = EXCLUDED.gap_to_market_p50,
        gap_to_market_p25 = EXCLUDED.gap_to_market_p25,
        lag1_ext_salary_p50 = EXCLUDED.lag1_ext_salary_p50,
        lag1_ext_salary_p25 = EXCLUDED.lag1_ext_salary_p25,
        lag1_turnover = EXCLUDED.lag1_turnover,
        rolling_mean_turnover_4w = EXCLUDED.rolling_mean_turnover_4w,
        n_weeks_12w = EXCLUDED.n_weeks_12w,
        avg_vacancy_12w = EXCLUDED.avg_vacancy_12w,
        lag1_post_cnt = EXCLUDED.lag1_post_cnt,
        rolling_mean_post_4w = EXCLUDED.rolling_mean_post_4w,
        avg_post_cnt_12w = EXCLUDED.avg_post_cnt_12w,
        lag52_ext_salary_p50 = EXCLUDED.lag52_ext_salary_p50,
        salary_p50_yoy_diff = EXCLUDED.salary_p50_yoy_diff,
        avg_ext_vacancy_w52 = EXCLUDED.avg_ext_vacancy_w52,
        avg_post_cnt_52w = EXCLUDED.avg_post_cnt_52w,
        vacancy_cnt_yoy_diff = EXCLUDED.vacancy_cnt_yoy_diff,
        lag52_turnover_coeff = EXCLUDED.lag52_turnover_coeff,
        turnover_yoy_diff = EXCLUDED.turnover_yoy_diff,
        lag52_gap_to_market_p50 = EXCLUDED.lag52_gap_to_market_p50,
        lag52_gap_to_market_p25 = EXCLUDED.lag52_gap_to_market_p25,
        gap_yoy_diff = EXCLUDED.gap_yoy_diff,
        nulls_ratio = EXCLUDED.nulls_ratio,
        data_insufficient = EXCLUDED.data_insufficient,
        inserted_at = now();

    INSERT INTO stg_sys.logging (prc_name, description_step)
    VALUES ('PRC_UPDATE_CITY_POSITION_FEATURES_GE', 'Данные обновлены успешно');
END;
$$;
