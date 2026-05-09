-- Databricks notebook source

 

/*---

-- ============================================================================

-- Model:        cur_coverage_reinsurance_dim

-- Version:      1.0.0

-- Release:      r2026.03.31

-- Logical Flow: INT_REINSURANCE (int) > REINSURANCE_DIM (cur) > SCD2

-- Description:  SCD Type 2 dimension load for reinsurance_dim. Derives one

--               distinct cession row per cov_reins_concat_bk from the

--               int_reinsurance layer. Tracks changes on reinsurance_plan_code

--               and cession_start_date. Reload-safe and self-healing.

--

-- Sources:

--   - reinsurance

--

-- Depends On:

--   - ref_extract_month

--   - int_reinsurance  (int-layer, must be loaded first)

--

-- Produces:

--   - MERGE INTO: cur_coverage_reinsurance_dim

--

-- Consumed By:

--   - downstream actuarial reporting / IFRS17 grouping

--

-- Owners:

--   Business:  Affinity Ops

--   Technical: GFT Actuarial Data Platform

--   Team:      GFT Engineering Services

--

-- Changelog:

--   2026-03-31  Feature: initial SCD2 all-timeline MERGE with reload support

--               Converted from IICS mapping VST_MCT_M_CANAFF_DW_COV_REINSURANCE_DIM

-- ============================================================================

---*/

 

-- COMMAND ----------

 

-- DBTITLE 1,Parameters: Widgets

CREATE WIDGET TEXT catalog_name          DEFAULT 'gfdo_dev_catalog';

CREATE WIDGET TEXT curated_database_name DEFAULT 'gf_actuarial_affinity_res_cur';

CREATE WIDGET TEXT batch_id              DEFAULT '001';

CREATE WIDGET TEXT etl_user              DEFAULT 'ETLPROC';

CREATE WIDGET TEXT reporting_period_sid  DEFAULT '';

CREATE WIDGET TEXT pipeline_name         DEFAULT 'MC';

 

-- COMMAND ----------

 

-- DBTITLE 1,Derived Variables

 

DECLARE OR REPLACE v_reporting_period_sid INT;

DECLARE OR REPLACE v_reporting_period     STRING;

DECLARE OR REPLACE v_etl_user             STRING;

DECLARE OR REPLACE v_batch_id             STRING;

DECLARE OR REPLACE v_pipeline_name        STRING;

 

-- 3-priority COALESCE for reporting_period_sid

SET VAR v_reporting_period_sid = CAST(

    COALESCE(

        -- 1st: widget override (if provided)

        NULLIF(TRIM(:reporting_period_sid), ''),

        -- 2nd: control table

        (SELECT reporting_period_sid

         FROM IDENTIFIER(CONCAT(:catalog_name, '.', :curated_database_name, '.', 'ref_extract_month'))

         WHERE curr_ind = 1),

        -- 3rd: computed fallback (last quarter-end)

        DATE_FORMAT(LAST_DAY(ADD_MONTHS(DATE_TRUNC('QUARTER', CURRENT_DATE()), -1)), 'yyyyMMdd')

    ) AS INT

);

 

SET VAR v_reporting_period = LEFT(CAST(v_reporting_period_sid AS STRING), 6);

SET VAR v_etl_user         = COALESCE(NULLIF(TRIM(:etl_user), ''), CURRENT_USER());

SET VAR v_batch_id         = COALESCE(NULLIF(TRIM(:batch_id), ''), DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMddHHmmss'));

SET VAR v_pipeline_name    = COALESCE(NULLIF(TRIM(:pipeline_name), ''), 'MC');

 

-- COMMAND ----------

 

-- DBTITLE 1,Step 1: SCD2 Load for cur_coverage_reinsurance_dim (all-timeline MERGE, reload-safe, self-healing)

-- ============================================================================

-- Purpose: Derive one distinct cession row per cov_reins_concat_bk from

--          int_reinsurance for the current reporting period. Build an

--          all-timeline SCD2 view, detect changes via LAG hash comparison

--          on the tracked attributes (reinsurance_plan_code, cession_start_date),

--          derive SCD2 fields (eff_dt, exp_dt, curr_ind), and MERGE.

--

-- Business key : cession_concat_bk  = TRIM(cov_reins_concat_bk)

-- Join key     : business_key_hash  = MD5(STRUCT(cession_concat_bk, eff_dt))

-- Tracked attrs: reinsurance_plan_code, cession_start_date

--               (mirrors IICS exp_compare: o_var_reins_plan_code | o_var_treaty_start_dt)

--

-- MERGE actions:

--   MATCHED + changed          → UPDATE all columns (exp_dt, curr_ind, record_hash)

--   NOT MATCHED BY SOURCE      → DELETE (phantom rows from bad loads, scoped to batch eff_dt)

--   NOT MATCHED BY TARGET      → INSERT new SCD2 version

-- ============================================================================

-- ============================================================================

 

WITH src_reinsurance AS (

    -- Source Qualifier: extract distinct cession rows from int_reinsurance

    -- for the current reporting period and pipeline. TRIM all VARCHAR columns.

    SELECT

        TRIM(r.cov_reins_concat_bk)    AS cession_concat_bk,

        TRIM(r.cov_reins_bk)           AS cession_bk,

        r.treaty_start_dt              AS cession_start_date,

        TRIM(r.reins_plan_cd)          AS reinsurance_plan_code,

        TRIM(r.line_of_business)       AS line_of_business_code_bk

    FROM IDENTIFIER(CONCAT(:catalog_name, '.', :curated_database_name, '.', 'int_reinsurance')) AS r

    WHERE r.reporting_period_sid = v_reporting_period_sid

      AND r.pipeline_name        = v_pipeline_name

      AND r.cov_reins_concat_bk  IS NOT NULL

      AND TRIM(r.cov_reins_concat_bk) <> ''

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY TRIM(r.cov_reins_concat_bk)

        ORDER BY r.cov_reins_id ASC

    ) = 1

),

 

transformed_source AS (

    -- Rename and shape columns into target names; derive eff_dt for this batch

    SELECT

        cession_concat_bk,

        cession_bk,

        cession_start_date,

        reinsurance_plan_code,

        line_of_business_code_bk,

        DATE_ADD(TO_DATE(CAST(v_reporting_period_sid AS STRING), 'yyyyMMdd'), 1) AS eff_dt

    FROM src_reinsurance

),

 

hashed_source AS (

    -- Add business_key_hash (SCD2 MERGE join key) and record_hash (change detection).

    -- record_hash tracks only the attributes IICS monitors for changes:

    --   reinsurance_plan_code and cession_start_date

    SELECT

        *,

        MD5(TO_JSON(STRUCT(cession_concat_bk, eff_dt))) AS business_key_hash,

        MD5(TO_JSON(STRUCT(cession_start_date, reinsurance_plan_code))) AS record_hash

    FROM transformed_source

),

 

deduped_timeline AS (

    -- Build SCD2 timeline: source rows (priority 1) UNION selective target rows (priority 0).

    --   Target union has two branches:

    --   (a) previously-active versions from prior periods — provides LAG continuity

    --   (b) rows capped at period-end by a prior run of this batch — predecessor restore on reload

    --   Dedup on (cession_concat_bk, eff_dt): source always wins over target.

    SELECT

        cession_sid,

        cession_concat_bk,

        cession_bk,

        cession_start_date,

        reinsurance_plan_code,

        line_of_business_code_bk,

        eff_dt,

        business_key_hash,

        record_hash

    FROM (

        SELECT

            XXHASH64(TO_JSON(STRUCT(cession_concat_bk, eff_dt))) & 9223372036854775807 AS cession_sid,

            cession_concat_bk,

            cession_bk,

            cession_start_date,

            reinsurance_plan_code,

            line_of_business_code_bk,

            eff_dt,

            business_key_hash,

            record_hash,

            1 AS merge_priority  -- source wins on reload

        FROM hashed_source

 

        UNION ALL

 

        SELECT

            cession_sid,

            cession_concat_bk,

            cession_bk,

            cession_start_date,

            reinsurance_plan_code,

            line_of_business_code_bk,

            eff_dt,

            business_key_hash,

            record_hash,

            0 AS merge_priority  -- target yields on reload

        FROM IDENTIFIER(CONCAT(:catalog_name, '.', :curated_database_name, '.', 'cur_coverage_reinsurance_dim'))

        WHERE pipeline_name = v_pipeline_name

          AND (

              -- (a) previous-period current rows — provides LAG continuity across periods

              (curr_ind = 1

               AND eff_dt < DATE_ADD(TO_DATE(CAST(v_reporting_period_sid AS STRING), 'yyyyMMdd'), 1))

              -- (b) rows capped at the period-end date by a prior run of this same batch

              --     (exp_dt = reporting_period_sid date). Self-heals bad loads.

              OR exp_dt = TO_DATE(CAST(v_reporting_period_sid AS STRING), 'yyyyMMdd')

          )

    ) AS timeline

    QUALIFY ROW_NUMBER() OVER (

        PARTITION BY cession_concat_bk, eff_dt

        ORDER BY merge_priority DESC

    ) = 1

),

 

possible_changes AS (

    -- Detect changes via LAG hash comparison across the full timeline per cession key

    SELECT

        *,

        LAG(record_hash) OVER (PARTITION BY cession_concat_bk ORDER BY eff_dt) AS prev_record_hash

    FROM deduped_timeline

),

 

confirmed_changes AS (

    -- Keep only first-ever rows and rows where tracked attributes changed;

    -- derive exp_dt and curr_ind using LEAD over the surviving timeline

    SELECT

        fc.*,

        COALESCE(

            DATE_SUB(LEAD(eff_dt) OVER (PARTITION BY cession_concat_bk ORDER BY eff_dt), 1),

            TO_DATE('9999-12-31')

        ) AS exp_dt,

        CASE

            WHEN LEAD(eff_dt) OVER (PARTITION BY cession_concat_bk ORDER BY eff_dt) IS NULL THEN 1

            ELSE 0

        END AS curr_ind,

        v_pipeline_name AS pipeline_name,

        CAST(v_batch_id AS BIGINT) AS batch_id

    FROM (

        SELECT

            cession_sid,

            cession_concat_bk,

            cession_bk,

            cession_start_date,

            reinsurance_plan_code,

            line_of_business_code_bk,

            eff_dt,

            business_key_hash,

            record_hash

        FROM possible_changes

        WHERE prev_record_hash IS NULL

           OR COALESCE(record_hash, '') <> COALESCE(prev_record_hash, '')

    ) AS fc

)

 

MERGE INTO IDENTIFIER(CONCAT(:catalog_name, '.', :curated_database_name, '.', 'cur_coverage_reinsurance_dim')) AS tgt

USING confirmed_changes AS src

    ON tgt.business_key_hash = src.business_key_hash

WHEN MATCHED AND (

        tgt.record_hash <> src.record_hash

        OR tgt.exp_dt   <> src.exp_dt

        OR tgt.curr_ind <> src.curr_ind

    ) THEN

    UPDATE SET

        tgt.cession_sid              = src.cession_sid,

        tgt.cession_start_date       = src.cession_start_date,

        tgt.reinsurance_plan_code    = src.reinsurance_plan_code,

        tgt.eff_dt                   = src.eff_dt,

        tgt.exp_dt                   = src.exp_dt,

        tgt.curr_ind                 = src.curr_ind,

        tgt.created_by_uid           = v_etl_user,

        tgt.created_ts               = CURRENT_DATE(),

        tgt.record_hash              = src.record_hash,

        tgt.pipeline_name            = src.pipeline_name,

        tgt.batch_id                 = src.batch_id

WHEN NOT MATCHED BY TARGET THEN

    INSERT (

        cession_sid,

        cession_concat_bk,

        cession_bk,

        cession_start_date,

        reinsurance_plan_code,

        line_of_business_code_bk,

        eff_dt,

        exp_dt,

        curr_ind,

        created_by_uid,

        created_ts,

        pipeline_name,

        business_key_hash,

        record_hash,

        batch_id

    )

    VALUES (

        src.cession_sid,

        src.cession_concat_bk,

        src.cession_bk,

        src.cession_start_date,

        src.reinsurance_plan_code,

        src.line_of_business_code_bk,

        src.eff_dt,

        src.exp_dt,

        src.curr_ind,

        v_etl_user,

        CURRENT_DATE(),

        src.pipeline_name,

        src.business_key_hash,

        src.record_hash,

        src.batch_id

    )

WHEN NOT MATCHED BY SOURCE

    AND tgt.pipeline_name = v_pipeline_name

    AND tgt.eff_dt = DATE_ADD(TO_DATE(CAST(v_reporting_period_sid AS STRING), 'yyyyMMdd'), 1)

  THEN DELETE

;

 