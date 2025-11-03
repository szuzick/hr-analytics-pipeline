-- Calculate attrition risk scoring for retention focus

WITH metrics AS (
    SELECT * FROM {{ ref('int_employee_metrics') }}
),

risk_factors AS (
    SELECT
        employee_number,
        attrition_status,
        attrition_flag,
        department,
        job_role,
        annual_income,
        
        -- Risk factor 1: Low satisfaction (2 points)
        CASE 
            WHEN job_satisfaction_level = 'Low' THEN 2
            WHEN job_satisfaction_level = 'Medium' THEN 1
            ELSE 0
        END as satisfaction_risk,
        
        -- Risk factor 2: Poor work-life balance (2 points)
        CASE 
            WHEN work_life_balance_level = 'Bad' THEN 2
            WHEN work_life_balance_level = 'Good' THEN 1
            ELSE 0
        END as worklife_risk,
        
        -- Risk factor 3: No recent promotion (2 points)
        CASE 
            WHEN years_since_last_promotion > 5 THEN 2
            WHEN years_since_last_promotion > 2 THEN 1
            ELSE 0
        END as promotion_risk,
        
        -- Risk factor 4: Overtime (1 point)
        CASE 
            WHEN works_overtime = TRUE THEN 1
            ELSE 0
        END as overtime_risk,
        
        -- Risk factor 5: Low income (2 points)
        CASE
            WHEN annual_income < 50000 THEN 2
            WHEN annual_income < 80000 THEN 1
            ELSE 0
        END as compensation_risk,
        
        -- Risk factor 6: Low performance (bonus risk)
        CASE 
            WHEN performance_level = 'Low' THEN 2
            ELSE 0
        END as performance_risk
        
    FROM metrics
),

total_risk AS (
    SELECT
        *,
        satisfaction_risk + worklife_risk + promotion_risk + 
        overtime_risk + compensation_risk + performance_risk as total_risk_score,
        
        CASE 
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 6 THEN 'Critical Risk'
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 4 THEN 'High Risk'
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 2 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END as risk_category,

        CASE 
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 6 THEN 4
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 4 THEN 3
            WHEN satisfaction_risk + worklife_risk + promotion_risk + 
                 overtime_risk + compensation_risk + performance_risk >= 2 THEN 2
            ELSE 1
        END as risk_sort_order
        
    FROM risk_factors
)

SELECT * FROM total_risk