-- Dimension table: Employee lookup table for filtering

SELECT
    employee_number,
    department,
    job_role,
    gender,
    education_level,
    job_level,
    marital_status,
    business_travel,
    
    -- Employee categorization
    age,
    CASE 
        WHEN age < 25 THEN 'Under 25'
        WHEN age < 35 THEN '25-34'
        WHEN age < 45 THEN '35-44'
        WHEN age < 55 THEN '45-54'
        ELSE '55+'
    END as age_group,
    
    total_working_years,
    years_at_company,
    CASE 
        WHEN years_at_company < 2 THEN 'New'
        WHEN years_at_company < 5 THEN 'Established'
        WHEN years_at_company < 10 THEN 'Experienced'
        ELSE 'Tenured'
    END as tenure_category

FROM {{ ref('stg_hr_employees') }}