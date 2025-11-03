-- Calculate tenure, compensation, and engagement metrics

WITH base AS (
    SELECT * FROM {{ ref('stg_hr_employees') }}
),

metrics AS (
    SELECT
        works_overtime,
        employee_number,
        attrition_status,
        attrition_flag,
        
        -- Age grouping
        CASE 
            WHEN age < 25 THEN 'Under 25'
            WHEN age < 35 THEN '25-34'
            WHEN age < 45 THEN '35-44'
            WHEN age < 55 THEN '45-54'
            ELSE '55+'
        END as age_group,
        
        -- Tenure categories (KPI)
        CASE 
            WHEN years_at_company < 2 THEN 'New (0-2 years)'
            WHEN years_at_company < 5 THEN 'Established (2-5 years)'
            WHEN years_at_company < 10 THEN 'Experienced (5-10 years)'
            ELSE 'Tenured (10+ years)'
        END as tenure_category,

        -- Tenure sort order for proper ordering
        CASE 
            WHEN years_at_company < 2 THEN 1
            WHEN years_at_company < 5 THEN 2
            WHEN years_at_company < 10 THEN 3
            ELSE 4
        END as tenure_sort_order,
        
        -- Income brackets (KPI)
        CASE 
            WHEN annual_income < 40000 THEN 'Under $40K'
            WHEN annual_income < 60000 THEN '$40K-$60K'
            WHEN annual_income < 80000 THEN '$60K-$80K'
            WHEN annual_income < 100000 THEN '$80K-$100K'
            ELSE '$100K+'
        END as income_bracket,
        
        -- Engagement score (KPI: average of satisfaction metrics)
        ROUND((
            CASE job_satisfaction_level
                WHEN 'Low' THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'High' THEN 3
                WHEN 'Very High' THEN 4
            END +
            CASE environment_satisfaction_level
                WHEN 'Low' THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'High' THEN 3
                WHEN 'Very High' THEN 4
            END +
            CASE work_life_balance_level
                WHEN 'Bad' THEN 1
                WHEN 'Good' THEN 2
                WHEN 'Better' THEN 3
                WHEN 'Best' THEN 4
            END +
            CASE job_involvement_level
                WHEN 'Low' THEN 1
                WHEN 'Medium' THEN 2
                WHEN 'High' THEN 3
                WHEN 'Very High' THEN 4
            END
        ) / 4.0, 2) as engagement_score,
        
        -- Promotion recency flag (KPI)
        CASE 
            WHEN years_since_last_promotion = 0 THEN 'Promoted This Year'
            WHEN years_since_last_promotion <= 2 THEN 'Recent Promotion'
            WHEN years_since_last_promotion <= 5 THEN 'Due for Review'
            ELSE 'Long Without Promotion'
        END as promotion_status,
        
        -- Development investment flag (KPI)
        CASE 
            WHEN training_times_last_year = 0 THEN 'No Training'
            WHEN training_times_last_year <= 2 THEN 'Minimal Training'
            WHEN training_times_last_year <= 5 THEN 'Regular Training'
            ELSE 'High Investment'
        END as training_level,

        -- Training investment score (NEW ENHANCEMENT v1.2)
        CASE 
            WHEN training_times_last_year = 0 THEN 0
            WHEN training_times_last_year <= 2 THEN 1
            WHEN training_times_last_year <= 5 THEN 2
            ELSE 3
        END as training_investment_score,
        
        -- Keep original fields
        age,
        annual_income,
        years_at_company,
        years_in_current_role,
        years_since_last_promotion,
        job_satisfaction_level,
        environment_satisfaction_level,
        work_life_balance_level,
        performance_level,
        department,
        job_role,
        gender,
        education_level
        
    FROM base
)

SELECT * FROM metrics