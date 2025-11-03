-- Clean and standardize raw employee data

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'hr_employees') }}
),

cleaned AS (
    SELECT
        employee_number,
        age,
        UPPER(TRIM(attrition)) as attrition_status,
        
        CASE 
            WHEN department LIKE '%Research%' THEN 'Research and Development'
            WHEN department LIKE '%Sales%' THEN 'Sales'
            WHEN department LIKE '%Human%' THEN 'Human Resources'
            ELSE TRIM(department)
        END as department,
        
        TRIM(job_role) as job_role,
        TRIM(gender) as gender,
        TRIM(education_field) as education_field,
        TRIM(marital_status) as marital_status,
        TRIM(business_travel) as business_travel,
        
        CASE education
            WHEN 1 THEN 'Below College'
            WHEN 2 THEN 'College'
            WHEN 3 THEN 'Bachelor'
            WHEN 4 THEN 'Master'
            WHEN 5 THEN 'Doctor'
        END as education_level,
        
        CASE job_satisfaction
            WHEN 1 THEN 'Low'
            WHEN 2 THEN 'Medium'
            WHEN 3 THEN 'High'
            WHEN 4 THEN 'Very High'
        END as job_satisfaction_level,
        
        CASE environment_satisfaction
            WHEN 1 THEN 'Low'
            WHEN 2 THEN 'Medium'
            WHEN 3 THEN 'High'
            WHEN 4 THEN 'Very High'
        END as environment_satisfaction_level,
        
        CASE work_life_balance
            WHEN 1 THEN 'Bad'
            WHEN 2 THEN 'Good'
            WHEN 3 THEN 'Better'
            WHEN 4 THEN 'Best'
        END as work_life_balance_level,
        
        CASE performance_rating
            WHEN 1 THEN 'Low'
            WHEN 2 THEN 'Good'
            WHEN 3 THEN 'Excellent'
            WHEN 4 THEN 'Outstanding'
        END as performance_level,
        
        CASE job_involvement
            WHEN 1 THEN 'Low'
            WHEN 2 THEN 'Medium'
            WHEN 3 THEN 'High'
            WHEN 4 THEN 'Very High'
        END as job_involvement_level,
        
        CASE relationship_satisfaction
            WHEN 1 THEN 'Low'
            WHEN 2 THEN 'Medium'
            WHEN 3 THEN 'High'
            WHEN 4 THEN 'Very High'
        END as relationship_satisfaction_level,
        
        distance_from_home,
        monthly_income,
        monthly_income * 12 as annual_income,
        job_level,
        num_companies_worked,
        percent_salary_hike,
        total_working_years,
        training_times_last_year,
        years_at_company,
        years_in_current_role,
        years_since_last_promotion,
        years_with_curr_manager,
        
        CASE WHEN UPPER(over_time) = 'YES' THEN TRUE ELSE FALSE END as works_overtime,
        CASE WHEN UPPER(attrition) = 'YES' THEN 1 ELSE 0 END as attrition_flag,
        
        CURRENT_TIMESTAMP as dbt_loaded_at
        
    FROM source
    WHERE employee_number IS NOT NULL
)

SELECT * FROM cleaned