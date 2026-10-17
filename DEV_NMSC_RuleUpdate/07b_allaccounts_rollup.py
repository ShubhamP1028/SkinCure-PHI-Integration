# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08b · AllAccounts rollup — the Salesforce monthly numbers

# COMMAND ----------

dbutils.widgets.text("catalog", "dev_pathology_data", "Catalog")
dbutils.widgets.text("window_start", "2026-04-01", "Window start (inclusive)")
dbutils.widgets.text("window_end",   "2026-10-01", "Window end (exclusive)")
dbutils.widgets.dropdown("wide_crosscheck", "no", ["no", "yes"],
                         "Wide cross-check (slow)")
C    = dbutils.widgets.get("catalog")
W0   = dbutils.widgets.get("window_start")
W1   = dbutils.widgets.get("window_end")
WIDE = dbutils.widgets.get("wide_crosscheck")
WINDOW = f"{W0}_{W1}"
print(f"catalog {C} · window {WINDOW} · wide cross-check {WIDE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The HIG query — unchanged

# COMMAND ----------

df_old = spark.sql(""" 
 WITH src_dtl AS (
    SELECT 
        act.account_id,
        cases.account_id AS case_account_id,
        cases.case_id,
        act.contract_effective_date,
        act.go_live_date,
        act.status,
        act.tentative_go_live_tableau_date,
        CASE 
            WHEN cases.payor_mix_type IN ('Medicare Advantage','Medicare','Commercial','Medicaid') THEN cases.payor_mix_type
            WHEN cases.payor_mix_type = 'Supplement' THEN 'Commercial'
            ELSE NULL 
        END AS payer,
        act.terminated_date,
        cases.master_status,
        cases.ContactId AS employee_id,
        cases.created_date AS case_created_date,
        cases.closed_date AS case_closed_date,
        cases.number_of_lesions AS lesions,
        ptl.sim_scheduled AS sim_scheduled_date,
        ptl.VOB_Awaiting_Re_verification,
        ptl.ptl_id,
        ptl.sim AS sim_date,
        ptl.patient_name,
        ptl.treatment_type,
        ptl.treatment_date,
        ptl.Days_Sim_to_New_Start,
        ptl.Days_VOB_Closed_to_Sim,
        ptl.created_date AS ptl_created_date,
        ptl.Prescribed_of_FXweek,
        pat.treatment_type pat_treatment_type,
        pat.practice AS pat_account_id
    FROM us_scdb_prod.comm_silver.dim_account_dtl act
    LEFT JOIN us_scdb_prod.comm_silver.fact_case_dtl cases 
        ON act.account_id = cases.account_id
    LEFT JOIN us_scdb_prod.comm_silver.fact_ptl_dtl ptl 
        ON cases.case_id = ptl.vob_case
    LEFT JOIN us_scdb_prod.comm_silver.fact_patient_dtl pat 
        ON pat.patient_treatment_log = ptl.ptl_id 
        AND pat.date = coalesce(ptl.treatment_date ,ptl.sim)
        AND pat.treatment_type IN ('Treatment','New Start','Simulation', 'End Of Treatment')
    WHERE act.contract_effective_date IS NOT NULL
   
),distinct_accounts AS (
    SELECT DISTINCT account_id, status, go_live_date, terminated_date, tentative_go_live_tableau_date
    FROM src_dtl
),
coastal_accounts AS (
    SELECT account_id, status, tentative_go_live_tableau_date
    FROM us_scdb_prod.comm_silver.dim_account_dtl
    WHERE name ILIKE '%Dermatology Associates of Coastal Carolina%'
),
distinct_cases AS (
    SELECT DISTINCT 
        case_account_id, case_id, employee_id, payer, status,
        case_created_date, case_closed_date, go_live_date,tentative_go_live_tableau_date,
        master_status, VOB_Awaiting_Re_verification
    FROM src_dtl
),

distinct_ptl AS (
    SELECT DISTINCT 
        case_account_id, pat_account_id, ptl_id, patient_name, null as  employee_id, payer, src_dtl.status,
        sim_date, sim_scheduled_date, treatment_type, treatment_date, pat_treatment_type,
        ptl_created_date, go_live_date, lesions, case_created_date, case_closed_date,tentative_go_live_tableau_date,
        Days_Sim_to_New_Start, Days_VOB_Closed_to_Sim, Prescribed_of_FXweek, a.status as pat_account_status,
        ROW_NUMBER() OVER (
           PARTITION BY pat_account_id, ptl_id ORDER BY treatment_date DESC
        ) AS rn
    FROM src_dtl
    LEFT JOIN (select distinct account_id , status from us_scdb_prod.comm_silver.dim_account_dtl) a
    on src_dtl.pat_account_id = a.account_id 
),


-- ============================================================
-- STEP 3: Time bucket lookup (used multiple times)
-- ============================================================
time_buckets AS (
    SELECT 
        CASE 
            WHEN dayofweek(date_trunc('month', start_date)) BETWEEN 2 AND 6 THEN to_date(date_trunc('month', start_date))
            ELSE to_date(next_day(date_trunc('month', start_date) - INTERVAL 1 DAY, 'Mon'))
        END AS date,
        --next_day(date_trunc('month', start_date) - INTERVAL 1 DAY, 'Mon') AS date,
        MAX(net_working_days) AS net_working_days,
        start_date
    FROM us_scdb_prod.comm_gold.rpt_map_time_bucket
    WHERE time_bucket = 'MTD' AND time_period = 'CURRENT'
    GROUP BY 1,start_date
),

-- ============================================================
-- STEP 4: Combine account-level metrics into single pass
-- ============================================================
account_metrics AS (
    SELECT 
        account_id,
        NULL AS employee_id,
        NULL AS payer,
        status,
        NULL AS tx,
        -- Net Live Location
        CASE WHEN status = 'Active' AND go_live_date IS NOT NULL 
             THEN to_date(go_live_date) END AS net_live_date,
        -- Terminated
        CASE WHEN status = 'Terminated' AND terminated_date IS NOT NULL 
             THEN to_date(terminated_date) END AS terminated_date,
        -- Upcoming Go Live
        CASE WHEN tentative_go_live_tableau_date IS NOT NULL 
             THEN to_date(tentative_go_live_tableau_date) END AS upcoming_go_live_date
    FROM distinct_accounts
),

net_live_loc AS (
    SELECT account_id,  null as employee_id, null as payer, net_live_date AS date, status, tx,
           'Net Live Location' AS metric, cast(COUNT(DISTINCT account_id) as INTEGER) AS value
    FROM account_metrics
    WHERE net_live_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

terminated_dtl AS (
    SELECT account_id,  null as employee_id, null as payer, terminated_date AS date, status, tx,
           'Terminated' AS metric, cast(COUNT(DISTINCT account_id) as INTEGER) AS value
    FROM account_metrics
    WHERE terminated_date IS NOT NULL
    GROUP BY 1,2,3,4,5,6
),

-- ============================================================
-- STEP 5: Combine VOB metrics (single scan of distinct_cases)
-- ============================================================
vob_base AS (
       SELECT c.*,
        CASE WHEN c.master_status IN ('Working','Approved','Approved Restricted','Denied')
                  AND c.case_created_date IS NOT NULL
                  AND c.VOB_Awaiting_Re_verification IS NULL THEN 1 ELSE 0 END AS is_vob,
        CASE WHEN c.master_status IN ('Approved','Approved Restricted','Denied')
                  AND c.case_created_date IS NOT NULL
                  AND c.case_closed_date IS NOT NULL
                  AND c.VOB_Awaiting_Re_verification IS NULL THEN 1 ELSE 0 END AS is_vob_closed      
    FROM distinct_cases c join us_scdb_prod.sc_salesforce_raw.case raw on raw.Id = c.case_id
    where raw.RecordTypeId = '0123h000000VHSlAAO'
),

vob_dtl AS (
    SELECT case_account_id as account_id, null as employee_id,null as payer, to_date(case_created_date) AS date, status, NULL AS tx,
           'OpenVOB' AS metric, COUNT(DISTINCT case_id) AS value
    FROM vob_base WHERE is_vob = 1
    GROUP BY 1,2,3,4,5,6
),

vob_closed_dtl AS (
    SELECT case_account_id as account_id, null as employee_id,null as payer, to_date(case_closed_date) AS date, status, NULL AS tx,
           'ClosedVOB' AS metric, COUNT(DISTINCT case_id) AS value
    FROM vob_base WHERE is_vob_closed = 1
    GROUP BY 1,2,3,4,5,6
),


vob_dtl_payer AS (
    SELECT case_account_id AS account_id, NULL AS employee_id, payer, TO_DATE(case_created_date) AS date, status, NULL AS tx,
           "OpenVOBCount_" || payer AS metric, COUNT(DISTINCT case_id) AS value
    FROM vob_base
    WHERE is_vob = 1
      AND payer IN ('Medicare','Commercial','Medicare Advantage')
    GROUP BY 1,2,3,4,5,6
),

vob_closed_dtl_payer AS (
    SELECT case_account_id AS account_id, NULL AS employee_id, payer, TO_DATE(case_closed_date) AS date, status, NULL AS tx,
           "ClosedVOBCount_" || payer AS metric, COUNT(DISTINCT case_id) AS value
    FROM vob_base
    WHERE is_vob_closed = 1
      AND payer IN ('Medicare','Commercial','Medicare Advantage')
    GROUP BY 1,2,3,4,5,6
),

-- ============================================================
-- STEP 6: Combine treatment/PTL metrics (single scan)
-- ============================================================
/*sim_dtl AS (
    SELECT pat_account_id, null as employee_id, null as payer, sim_date AS date, status, NULL AS tx, -------PAYER INDEPENDENT (NULL)
           'SIM' AS metric, COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl WHERE sim_date IS NOT NULL 
    and pat_treatment_type = 'Simulation'
    GROUP BY 1,2,3,4,5,6
),*/

sim_dtl AS (
    SELECT pat_account_id as account_id, NULL AS employee_id, NULL AS payer,  sim_date AS date, pat_account_status as status,  NULL AS tx,
        'SIM' AS metric,
        COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl
    WHERE sim_date IS NOT NULL  AND pat_treatment_type = 'Simulation'
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT  a.account_id AS pat_account_id, NULL AS employee_id, NULL AS payer, a.date_of_service AS date, c.status, NULL AS tx,
        'SIM' AS metric,
        a.Total_Sims AS value
    FROM us_scdb_prod.comm_silver.fact_adc_dtl a
JOIN coastal_accounts c
    ON a.account_id = c.account_id
WHERE a.Total_Sims > 0
)
,
sim_dtl_2 AS (
    SELECT pat_account_id as account_id, null as employee_id, payer, sim_date AS date,pat_account_status as  status, NULL AS tx, ---------PAYER DEPENDENT SIM
           'SIM_payer' AS metric, COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl WHERE sim_date IS NOT NULL 
    and pat_treatment_type = 'Simulation'
    GROUP BY 1,2,3,4,5,6
),  


new_start_dtl AS (
    SELECT pat_account_id AS account_id,  NULL AS employee_id,  NULL AS payer, TO_DATE(treatment_date) AS date,pat_account_status as   status,  NULL AS tx,
        'New Start' AS metric,
        COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl
    WHERE treatment_type = 'Tx_1__c'  AND treatment_date IS NOT NULL  AND ptl_created_date IS NOT NULL  AND pat_treatment_type not in ('Simulation' , 'End Of Treatment')
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT a.account_id,   NULL AS employee_id, NULL AS payer,  a.date_of_service AS date, c.status,  NULL AS tx,
        'New Start' AS metric,
        a.Total_New_Starts AS value
    FROM us_scdb_prod.comm_silver.fact_adc_dtl a
    JOIN coastal_accounts c
        ON a.account_id = c.account_id
    WHERE a.Total_New_Starts > 0
),

new_start_dtl_2 AS (
    SELECT pat_account_id AS account_id, NULL AS employee_id, payer, --------PAYER DEPENDENT
           to_date(treatment_date) AS date,pat_account_status as  status, NULL AS tx,
           'New Start_payer' AS metric, COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl 
    WHERE treatment_type = 'Tx_1__c' 
        AND treatment_date IS NOT NULL 
        AND ptl_created_date IS NOT NULL 
        and pat_treatment_type not in ('Simulation' , 'End Of Treatment')
    GROUP BY 1,2,3,4,5
),

fraction_dtl AS (
    SELECT pat_account_id AS account_id, null as employee_id, NULL AS payer, 
           to_date(treatment_date) AS date, pat_account_status as  status, NULL AS tx,
           'Fraction' AS metric, COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl 
    WHERE treatment_date IS NOT NULL AND ptl_created_date IS NOT NULL 
    and pat_treatment_type not in ('Simulation' , 'End Of Treatment')
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT  a.account_id AS pat_account_id, NULL AS employee_id, NULL AS payer, a.date_of_service AS date, c.status, NULL AS tx,
        'Fraction' AS metric,
        a.Total_Treatments AS value
    FROM us_scdb_prod.comm_silver.fact_adc_dtl a
JOIN coastal_accounts c
    ON a.account_id = c.account_id
WHERE a.Total_Treatments > 0
),

fraction_dtl_2 AS (
    SELECT pat_account_id AS account_id, null as employee_id, payer, 
           to_date(treatment_date) AS date, pat_account_status as  status, NULL AS tx,
           'Fraction_payer' AS metric, COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl 
    WHERE treatment_date IS NOT NULL AND ptl_created_date IS NOT NULL 
    and pat_treatment_type not in ('Simulation' , 'End Of Treatment')
    GROUP BY 1,2,3,4,5,6
),



-- ============================================================
-- STEP 7: ADC calculations with window functions (avoid self-joins)
-- ============================================================
fraction_monthly AS (
    SELECT 
        account_id,
        CASE 
            WHEN dayofweek(date_trunc('month', date)) BETWEEN 2 AND 6 THEN to_date(date_trunc('month', date))
            ELSE to_date(next_day(date_trunc('month', date) - INTERVAL 1 DAY, 'Mon'))
        END AS month_date,
        --next_day(date_trunc('month', date) - INTERVAL 1 DAY, 'Mon') AS month_date,
        status,
        SUM(value) AS fraction_value
    FROM fraction_dtl
    GROUP BY 1,2,3
),

adc_base AS (
    SELECT 
        d.pat_account_id AS account_id,
        t.date AS date,
        d.status,
        COALESCE(f.fraction_value, 0) AS fraction_value,
        t.net_working_days,
        a.adc_value AS historical_adc
    FROM (
        SELECT DISTINCT
            coalesce(pat_account_id,case_account_id) as pat_account_id ,
            case when pat_account_id is not null then pat_account_status else status end as status
        FROM distinct_ptl
     ) d
    CROSS JOIN time_buckets t
    LEFT JOIN fraction_monthly f
        ON d.pat_account_id = f.account_id
       AND d.status = f.status
       AND t.date = f.month_date
    LEFT JOIN (
        SELECT 
            account_id,
            CASE 
                WHEN dayofweek(date_trunc('month', date_of_service)) BETWEEN 2 AND 6 
                THEN to_date(date_trunc('month', date_of_service))
                ELSE to_date(
                    next_day(
                        date_trunc('month', date_of_service) - INTERVAL 1 DAY,
                        'Mon'
                    )
                )
            END AS date,
            AVG(daily_adc) AS adc_value
        --FROM us_scdb_prod.comm_silver.fact_adc_dtl where latest_adc_of_month= 'true' 
        FROM ( SELECT *,
               ROW_NUMBER() OVER ( PARTITION BY account_id, date_trunc('month', date_of_service)  ORDER BY date_of_service DESC  ) AS rn
        FROM us_scdb_prod.comm_silver.fact_adc_dtl  ) adc
        WHERE rn = 1
        GROUP BY 1,2) a
        ON a.account_id = d.pat_account_id
       AND a.date = t.date
),

adc_with_trailing AS (
    SELECT *,
        CAST(CASE WHEN fraction_value > 0 THEN fraction_value / net_working_days 
                  ELSE historical_adc END AS DOUBLE) AS adc_value,
        -- Window functions for trailing calculations
        AVG(CASE WHEN fraction_value > 0 THEN fraction_value / net_working_days 
                 ELSE historical_adc END) 
            OVER (PARTITION BY account_id, status 
                  ORDER BY date 
                  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS adc_3m,
        AVG(CASE WHEN fraction_value > 0 THEN fraction_value / net_working_days 
                 ELSE historical_adc END) 
            OVER (PARTITION BY account_id, status 
                  ORDER BY date     
                  ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS adc_6m,
        AVG(CASE WHEN fraction_value > 0 THEN fraction_value / net_working_days 
                 ELSE historical_adc END) 
            OVER (PARTITION BY account_id, status 
                  ORDER BY date 
                  ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS adc_12m
    FROM adc_base
    where (fraction_value <> 0 or historical_adc <> 0)
),

adc_dtl AS (
    SELECT account_id, NULL AS employee_id, NULL AS payer, date, status, NULL AS tx,
           'ADC Monthly' AS metric, adc_value AS value
    FROM adc_with_trailing
),


-- AVG days from VOB close to simulation -- Pam's column AY.
-- Emitted as a SUM and a COUNT rather than as a per-case day value, for two reasons:
--   1. `all_metric_src` below does `sum(dtl.value)` over the month. Summing per-case
--      day counts gives a total, not an average. A sum and a count survive that sum
--      intact, and `_rollup_new` divides them -- a true case-weighted average.
--   2. The old inner `select distinct` omitted ptl_id, so two cases closing on the same
--      day with the same day count collapsed into one row. Keying on ptl_id keeps both.
-- The date is the VOB case CLOSED date, so the month is the closed month, not the sim
-- month. A case closed in July whose sim lands in August is a July row.
avg_vob_close_sim AS (
    SELECT case_account_id AS account_id, NULL AS employee_id, NULL AS payer,
           to_date(case_closed_date) AS date, status, NULL AS tx,
           'V2S_DaysSum' AS metric, sum(Days_VOB_Closed_to_Sim) AS value
    FROM (SELECT DISTINCT case_account_id, ptl_id, case_closed_date, status,
                          Days_VOB_Closed_to_Sim
          FROM distinct_ptl
          WHERE case_closed_date IS NOT NULL
            AND Days_VOB_Closed_to_Sim IS NOT NULL
            AND Days_VOB_Closed_to_Sim >= 0)
    GROUP BY 1,2,3,4,5,6,7
    UNION ALL
    SELECT case_account_id AS account_id, NULL AS employee_id, NULL AS payer,
           to_date(case_closed_date) AS date, status, NULL AS tx,
           'V2S_Cases' AS metric, count(*) AS value
    FROM (SELECT DISTINCT case_account_id, ptl_id, case_closed_date, status,
                          Days_VOB_Closed_to_Sim
          FROM distinct_ptl
          WHERE case_closed_date IS NOT NULL
            AND Days_VOB_Closed_to_Sim IS NOT NULL
            AND Days_VOB_Closed_to_Sim >= 0)
    GROUP BY 1,2,3,4,5,6,7
),

avg_sim_to_new_start AS (
    SELECT case_account_id as account_id, NULL AS employee_id, NULL AS payer,
           to_date(case_closed_date) AS date, status, NULL AS tx,
           'Days_Sim_to_New_Start' AS metric, Days_Sim_to_New_Start AS value
    FROM (select distinct case_account_id, case_closed_date,status,Days_Sim_to_New_Start from  distinct_ptl)
    WHERE case_closed_date IS NOT NULL AND Days_Sim_to_New_Start IS NOT NULL and Days_Sim_to_New_Start>=0
    GROUP BY 1,2,3,4,5,6,7,8
),
eot_dtl AS (
    SELECT
        pat_account_id AS account_id,  NULL AS employee_id,null as  payer, TO_DATE(treatment_date) AS date, pat_account_status AS status,
        NULL AS tx,
        'EOT' AS metric,
        COUNT(DISTINCT patient_name) AS value
    FROM distinct_ptl d
    WHERE pat_treatment_type = 'End Of Treatment'
       OR (
            (pat_account_id, ptl_id, treatment_date) IN (
                SELECT x.pat_account_id, x.ptl_id, MAX(x.treatment_date)
                FROM distinct_ptl x
                WHERE x.ptl_id = d.ptl_id   AND x.pat_account_id = d.pat_account_id  AND x.pat_treatment_type = 'Treatment'
                GROUP BY x.ptl_id, x.pat_account_id   )
            AND DATEDIFF(date_format(from_utc_timestamp(current_timestamp(), 'America/New_York'), 'yyyy-MM-dd'), treatment_date) >= 21
            AND NOT EXISTS (
                SELECT 1
                FROM distinct_ptl x
                WHERE x.pat_account_id = d.pat_account_id  AND x.ptl_id = d.ptl_id  AND x.pat_treatment_type = 'End Of Treatment'  )       )
    GROUP BY 1,2,3,4,5,6
    UNION ALL
    SELECT
        a.account_id,  NULL AS employee_id,null as   payer,  a.date_of_service AS date, c.status, NULL AS tx,
        'EOT' AS metric,
        a.End_of_Treatments AS value
    FROM us_scdb_prod.comm_silver.fact_adc_dtl a
    JOIN coastal_accounts c
        ON a.account_id = c.account_id
    WHERE a.End_of_Treatments > 0
),





-- ============================================================
-- FINAL UNION
-- ============================================================


final_src_dtl as (
      select * from vob_dtl
      union all
      select * from sim_dtl
      union all
      select * from new_start_dtl
      union all
      select * from adc_dtl
      union all
    select * from eot_dtl
    union all 
    select * from vob_closed_dtl
    union all 
    select * from fraction_dtl
     UNION ALL 
     SELECT * FROM vob_dtl_payer
    UNION ALL 
    SELECT * FROM vob_closed_dtl_payer
    UNION ALL
    -- [Sep 2026] the days-to-sim CTE was defined but never unioned in, so the metric
    -- never reached the pull. Column AY read blank for every account.
    SELECT * FROM avg_vob_close_sim
)



,
cutoff AS (
    SELECT date_sub(next_day(MAX(date), 'sat'), 7) AS last_full_week_end
    FROM final_src_dtl
    WHERE date <= current_date()
),
all_metric_src as (
  select dtl.account_id ,
   dim.account_name,
   sc_act.Account_Transition,
   sc_act.status,
   dim.senior_regional_name as senior_director,
   dim.regional_manager_name,
   dim.Regional_Focus_Status as mohs_offered_in_practice,
   dim.ABN_Utilization as trajectory,
   dim.go_live_date
  , dtl.metric  as metric
  ,to_date(date_trunc('month',dtl.date)) as mth_dt,
    sum(dtl.value) as value
  from final_src_dtl dtl
  left join us_scdb_prod.comm_silver.dim_account_dtl as sc_act
    on sc_act.account_id = dtl.account_id
  inner join us_scdb_prod.comm_gold.rpt_account_dim dim
    on dim.account_id = dtl.account_id
   --and dtl.status ilike 'Active'
   and dtl.date BETWEEN
           date_trunc('month', add_months(current_date(), -36))
       AND (SELECT last_full_week_end FROM cutoff)          -- dynamic: last full week in data
  inner join (
        select distinct Net_working_days, start_date, end_date
        from us_scdb_prod.comm_gold.rpt_map_time_bucket
        where time_bucket = 'MTD'
          and time_period ilike 'Current'
          and date BETWEEN
                  date_trunc('month', add_months(current_date(), -36))
              AND (SELECT last_full_week_end FROM cutoff)    -- must match the filter above
       ) bkt
    on dtl.date between bkt.start_date and bkt.end_date
 --where dtl.metric in ('OpenVOB','ClosedVOB','SIM','New Start','ADC Monthly','EOT','Fraction','ClosedVOBCount_','OpenVOBCount_')
  group by 1,2,3,4,5,6,7,8,9, dtl.metric, mth_dt, sc_act.Account_Transition
),

all_metric as (
    select * from all_metric_src
)



select src.account_id ,src.account_name,src.Account_Transition,src.status,src.senior_director,src.regional_manager_name,go_live_date,src.metric,src.mth_dt ,src.value, raw.Date_of_Last_Patient__c as DOLP from all_metric src
inner join us_scdb_prod.sc_salesforce_raw.account raw on raw.Id = src.account_id
where src.go_live_date is not null 
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Land the pull once — this is the whole optimisation

# COMMAND ----------

import time as _time

SRC = f"{C}.raw.allaccounts_src"

_t0 = _time.time()
(df_old.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(SRC))
_elapsed = _time.time() - _t0

_src_n = spark.sql(f"SELECT count(*) n FROM {SRC}").first()["n"]
print(f"{SRC}: {_src_n:,} rows · the HIG query ran once, in {_elapsed:,.0f}s")
print("everything below reads this table, not the query")

display(spark.sql(f"SELECT * FROM {SRC} LIMIT 200"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Long form — one row per account per month

# COMMAND ----------

# An exact-equality filter fails SILENTLY if the source renames a metric -- zero rows
# rather than an error, which would publish a portfolio that verified nobody. So the
# names are asserted before anything is read.
_metrics = [r["metric"] for r in
            spark.sql(f"SELECT DISTINCT metric FROM {SRC} ORDER BY metric").collect()]
print(f"{len(_metrics)} metric(s) in the pull: " + ", ".join(_metrics))
for _m in ("OpenVOB", "SIM", "V2S_DaysSum", "V2S_Cases"):
    assert _m in _metrics, (
        f"the pull has no metric named '{_m}'. It was renamed upstream, or the query "
        f"changed. Metrics present: {', '.join(_metrics)}")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _rollup_new AS
SELECT
  cast(account_id AS STRING)                                          AS account_id,
  date_format(to_date(mth_dt), 'yyyy-MM')                             AS month,
  cast(round(coalesce(sum(CASE WHEN metric = 'OpenVOB' THEN value END), 0)) AS BIGINT) AS vob,
  cast(round(coalesce(sum(CASE WHEN metric = 'SIM'     THEN value END), 0)) AS BIGINT) AS sim,
  -- Pam's column AY. Case-weighted: total days over total cases, NOT an average of
  -- averages. Deliberately NOT coalesced to 0 -- a month with nothing closed has to
  -- stay NULL. A zero here reads as "closed and simulated the same day", which is the
  -- ambiguity the pivoted All-Accounts CSV carries (168 of its 483 July rows).
  round(sum(CASE WHEN metric = 'V2S_DaysSum' THEN value END)
        / nullif(sum(CASE WHEN metric = 'V2S_Cases' THEN value END), 0), 1)
                                                                      AS avg_days_vob_close_to_sim,
  -- how many cases sit behind that average, so a one-case month is visible
  cast(coalesce(sum(CASE WHEN metric = 'V2S_Cases' THEN value END), 0) AS BIGINT)
                                                                      AS v2s_cases
FROM {SRC}
WHERE metric IN ('OpenVOB', 'SIM', 'V2S_DaysSum', 'V2S_Cases')
  AND to_date(mth_dt) IS NOT NULL
  AND account_id IS NOT NULL
GROUP BY 1, 2
""")

_m = spark.sql("SELECT count(DISTINCT month) n, min(month) lo, max(month) hi "
               "FROM _rollup_new").first()
assert _m["n"], "no months came back from the pull"
print(f"{_m['n']} month(s): {_m['lo']} .. {_m['hi']}")

# A month present on one side and not the other reads 0 rather than null, so the
# arithmetic downstream never has to special-case it.
_lop = spark.sql("""
SELECT month, sum(CASE WHEN vob = 0 THEN 1 ELSE 0 END) AS accounts_no_vob,
              sum(CASE WHEN sim = 0 THEN 1 ELSE 0 END) AS accounts_no_sim,
              sum(CASE WHEN avg_days_vob_close_to_sim IS NULL THEN 1 ELSE 0 END)
                                                        AS accounts_no_v2s,
              round(avg(avg_days_vob_close_to_sim), 1)  AS v2s_days_mean,
              sum(v2s_cases)                            AS v2s_cases,
              count(*) AS accounts
FROM _rollup_new GROUP BY month ORDER BY month
""")
display(_lop)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The wide cross-check — optional

# COMMAND ----------

if WIDE == "yes":
    from pyspark.sql import functions as F

    _months = [r["month"] for r in
               spark.sql("SELECT DISTINCT month FROM _rollup_new ORDER BY month").collect()]
    _vals = [f"OpenVOB_{m}" for m in _months] + [f"SIM_{m}" for m in _months]

    _w = (spark.table(SRC)
          .withColumn("metric_month",
                      F.concat_ws("_", F.col("metric"),
                                  F.date_format(F.to_date("mth_dt"), "yyyy-MM")))
          .groupBy("account_id", "account_name", "Account_Transition", "status",
                   "senior_director", "regional_manager_name", "go_live_date", "DOLP")
          .pivot("metric_month", _vals)      # explicit values -- no DISTINCT scan
          .agg(F.sum("value"))
          .fillna(0))

    _wv = _w.select(sum([F.coalesce(F.col(f"`OpenVOB_{m}`"), F.lit(0)) for m in _months],
                        F.lit(0)).alias("t")).groupBy().sum("t").collect()[0][0]
    _ws = _w.select(sum([F.coalesce(F.col(f"`SIM_{m}`"), F.lit(0)) for m in _months],
                        F.lit(0)).alias("t")).groupBy().sum("t").collect()[0][0]
    _lv = spark.sql("SELECT sum(vob) v, sum(sim) s FROM _rollup_new").first()

    print(f"  wide OpenVOB_* {int(_wv):,} vs long {_lv['v']:,}")
    print(f"  wide SIM_*     {int(_ws):,} vs long {_lv['s']:,}")
    assert int(round(_wv)) == int(_lv["v"]) and int(round(_ws)) == int(_lv["s"]), (
        "the wide and long forms disagree — the metric names or the payer split have "
        "changed upstream")
    display(_w.select(["account_id", "account_name"] + [f"OpenVOB_{m}" for m in _months]))
else:
    print("wide cross-check skipped (wide_crosscheck = no). Turn it on after any "
          "change to the HIG query.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Checks before anything is written

# COMMAND ----------

chk = spark.sql("""
SELECT count(*) AS rows, count(DISTINCT account_id) AS accounts,
       count(DISTINCT month) AS months, min(month) AS lo, max(month) AS hi,
       sum(vob) AS vob, sum(sim) AS sim,
       sum(CASE WHEN vob < 0 OR sim < 0 THEN 1 ELSE 0 END) AS negative
FROM _rollup_new
""").first()
print(f"  {chk['rows']:,} rows · {chk['accounts']:,} accounts · {chk['months']} months "
      f"({chk['lo']} .. {chk['hi']})")
print(f"  {chk['vob']:,} VOB · {chk['sim']:,} SIM")
assert chk["negative"] == 0, f"{chk['negative']} row(s) carry a negative count"

# The source filters days >= 0, so a negative average means the join changed, not the
# data. An exact 0.0 means a coalesce crept back in -- absent must stay absent.
_v2s = spark.sql("""
SELECT sum(CASE WHEN avg_days_vob_close_to_sim < 0 THEN 1 ELSE 0 END)  AS neg,
       sum(CASE WHEN avg_days_vob_close_to_sim = 0 THEN 1 ELSE 0 END)  AS zero,
       count(avg_days_vob_close_to_sim)                                AS have,
       count(*)                                                        AS rows
FROM _rollup_new
""").first()
assert _v2s["neg"] == 0, (
    f"{_v2s['neg']} account-month(s) carry a negative VOB-close-to-Sim average")
if _v2s["zero"]:
    print(f"  ** {_v2s['zero']} account-month(s) average EXACTLY 0 days -- a fabricated "
          f"zero, not an absent month. Check for a coalesce on the metric.")
print(f"  VOB close -> Sim days on {_v2s['have']:,} of {_v2s['rows']:,} account-months")

# One row per account per month. Structural now that the grain is the GROUP BY rather
# than a pivot over eight meta columns -- an account_id carrying two spellings of its
# own name used to emit two rows. Kept because it is the assumption every downstream
# sum makes, and it costs nothing.
_d = spark.sql("SELECT account_id, month, count(*) n FROM _rollup_new "
               "GROUP BY 1,2 HAVING count(*) > 1")
_nd = _d.count()
if _nd:
    display(_d)
assert _nd == 0, f"{_nd} (account_id, month) pair(s) appear more than once"

# [29 Aug] The "more sims than VOBs" warning is gone. These are the Tableau monthly. See wiki.
_sg = spark.sql("SELECT count(*) n FROM _rollup_new WHERE sim > vob").first()["n"]
print(f"  {_sg:,} account-month(s) carry more simulations than verifications — "
      f"expected, the two series key on different dates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Overwrite, not merge

# COMMAND ----------

# The window's months must all be in the pull, or the overwrite deletes one. See wiki.
_need = [r["m"] for r in spark.sql(f"""
SELECT date_format(d, 'yyyy-MM') AS m
FROM (SELECT explode(sequence(to_date('{W0}'),
                              add_months(to_date('{W1}'), -1),
                              interval 1 month)) AS d)
ORDER BY m
""").collect()]
_have = {r["month"] for r in spark.sql("SELECT DISTINCT month FROM _rollup_new").collect()}
_missing = [m for m in _need if m not in _have]
assert not _missing, (
    f"the pull is missing {', '.join(_missing)}, which the analysis window covers. "
    f"Overwriting would delete {'that month' if len(_missing) == 1 else 'those months'} "
    f"and every account would publish zero verifications for "
    f"{'it' if len(_missing) == 1 else 'them'}. Re-run the pull; do not write.")
print(f"  window months all present in the pull: {', '.join(_need)}")

SOURCE_LABEL = "HIG All Account Months YTD (New Logic)"

(spark.sql(f"""
 SELECT account_id, month, vob, sim,
        avg_days_vob_close_to_sim,
        v2s_cases,
        '{SOURCE_LABEL}'   AS source,
        current_timestamp() AS loaded_at
 FROM _rollup_new
 """)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{C}.raw.account_rollup"))

print(f"wrote {C}.raw.account_rollup: {chk['rows']:,} rows, "
      f"{chk['months']} month(s), replacing what was there")

display(spark.sql(f"""
SELECT month, count(*) AS accounts, sum(vob) AS vob, sum(sim) AS sim,
       round(sum(sim) / nullif(sum(vob), 0), 3) AS vob_to_sim,
       count(avg_days_vob_close_to_sim) AS accounts_with_v2s,
       round(sum(avg_days_vob_close_to_sim * v2s_cases) / nullif(sum(v2s_cases), 0), 1)
                                        AS v2s_days,
       max(loaded_at) AS last_loaded
FROM {C}.raw.account_rollup GROUP BY month ORDER BY month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Does every account we report on have numbers?

# COMMAND ----------

gap = spark.sql(f"""
WITH mapped AS (SELECT DISTINCT folder_name, account_id FROM {C}.raw.account_map),
     win    AS (SELECT explode(sequence(to_date('{W0}'),
                                        add_months(to_date('{W1}'), -1),
                                        interval 1 month)) AS d)
SELECT m.folder_name, m.account_id, date_format(w.d, 'yyyy-MM') AS month
FROM mapped m CROSS JOIN win w
LEFT JOIN {C}.raw.account_rollup r
       ON r.account_id = m.account_id AND r.month = date_format(w.d, 'yyyy-MM')
WHERE r.account_id IS NULL
ORDER BY m.folder_name, month
""")
_ng = gap.count()
if _ng:
    display(gap)
    print(f"  *** {_ng} (account, month) combination(s) in the map have no rollup row "
          f"and will publish as zero verifications.")
    print(f"  *** If the account genuinely raised none that month, insert an explicit "
          f"zero so the sheet states it rather than guessing.")
else:
    print("  every mapped account has a rollup row for every month of the window")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next