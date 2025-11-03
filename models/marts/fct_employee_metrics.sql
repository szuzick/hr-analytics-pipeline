-- Fact table: Employee metrics ready for Power BI dashboards

SELECT
    e.employee_number,
    e.attrition_status,
    e.attrition_flag,
    -- Employee attributes
    e.age,
    e.age_group,
    e.gender,
    e.education_level,
    e.department,
    e.job_role,
    -- Compensation (KPI)
    e.annual_income,
    e.income_bracket,
    -- Tenure metrics (KPI)
    e.years_at_company,
    e.tenure_category,
    e.tenure_sort_order,  -- ADD THIS LINE
    e.years_in_current_role,
    e.years_since_last_promotion,
    e.promotion_status,
    -- Engagement metrics (KPI)
    e.job_satisfaction_level,
    e.environment_satisfaction_level,
    e.work_life_balance_level,
    e.engagement_score,
    -- Performance metrics (KPI)
    e.performance_level,
    e.training_investment_score,
    -- Work characteristics
    e.works_overtime,
    -- Risk assessment (KPI)
    r.total_risk_score,
    r.risk_category,
    r.risk_sort_order,  -- ADD THIS LINE
    r.satisfaction_risk,
    r.worklife_risk,
    r.promotion_risk,
    r.overtime_risk,
    r.compensation_risk,
    r.performance_risk
FROM {{ ref('int_employee_metrics') }} e
LEFT JOIN {{ ref('int_employee_risk_scoring') }} r
    ON e.employee_number = r.employee_number