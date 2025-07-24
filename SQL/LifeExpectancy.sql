-- =====================================================
-- LIFE EXPECTANCY DATA ANALYSIS: COMPLETE POSTGRESQL WORKFLOW
-- =====================================================
-- Project: Global Life Expectancy Analysis (2000-2015)
-- Author: Lloyd Dela Cruz
-- Date: Dec 2024
-- Database: PostgreSQL
-- Tool: PostgreSQL extension; Claude AI; ChatGPT; Perplexity AI 
-- 
-- PROJECT STRUCTURE:
-- 1. Database Setup and Data Loading
-- 2. Initial Data Exploration
-- 3. Data Quality Assessment
-- 4. Data Cleaning and Standardization
-- 5. External Data Integration and Validation
-- 6. Feature Engineering
-- 7. Analytical Insights
-- 8. Final Dashboard Data Preparation
--
-- WHY THIS ANALYSIS MATTERS:
-- Life expectancy is the most fundamental measure of population health.
-- This analysis identifies successful health policies, efficient healthcare
-- systems, and critical factors that can guide interventions to save lives.
-- =====================================================

-- =====================================================
-- SECTION 1: DATABASE SETUP AND DATA LOADING
-- =====================================================
-- Purpose: Create database structure and load raw data
-- Note: Adjust file path based on your local setup

-- 1.1 Create Database (Run in psql or pgAdmin)
-- CREATE DATABASE life_expectancy_analysis;
-- \c life_expectancy_analysis;

-- 1.2 Create Schema for Organization
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS cleaned_data;
CREATE SCHEMA IF NOT EXISTS external_data;
CREATE SCHEMA IF NOT EXISTS analytics;

-- 1.3 Create Raw Data Table
DROP TABLE IF EXISTS raw_data.life_expectancy CASCADE;
CREATE TABLE raw_data.life_expectancy (
    country VARCHAR(255),
    year INTEGER,
    status VARCHAR(50),
    life_expectancy NUMERIC(5,2),
    adult_mortality NUMERIC(8,2),
    infant_deaths INTEGER,
    alcohol NUMERIC(5,2),
    percentage_expenditure NUMERIC(10,2),
    hepatitis_b NUMERIC(5,2),
    measles INTEGER,
    bmi NUMERIC(5,2),
    under_five_deaths INTEGER,
    polio NUMERIC(5,2),
    total_expenditure NUMERIC(5,2),
    diphtheria NUMERIC(5,2),
    hiv_aids NUMERIC(5,2),
    gdp NUMERIC(15,2),
    population BIGINT,
    thinness_1_19_years NUMERIC(5,2),
    thinness_5_9_years NUMERIC(5,2),
    income_composition_of_resources NUMERIC(5,3),
    schooling NUMERIC(4,1)
);

-- 1.4 Load Data from CSV
-- Note: Update the file path to match your local setup
-- For Mac: '/Users/Lloyd/Documents/Life_Expectancy_Data.csv'

-- COPY raw_data.life_expectancy
-- FROM '/path/to/your/Life_Expectancy_Data.csv'
-- WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '');

-- Alternative: If using VS Code PostgreSQL extension, you can use PSQL import wizard

-- 1.5 Verify Data Load
SELECT 
    'Total Records' as metric,
    COUNT(*) as value
FROM raw_data.life_expectancy
UNION ALL
SELECT 
    'Unique Countries',
    COUNT(DISTINCT country)
FROM raw_data.life_expectancy
UNION ALL
SELECT 
    'Years Covered',
    COUNT(DISTINCT year)
FROM raw_data.life_expectancy
UNION ALL
SELECT 
    'Expected Records (193 countries × 16 years)',
    193 * 16;

-- =====================================================
-- SECTION 2: INITIAL DATA EXPLORATION
-- =====================================================
-- Purpose: Understand dataset structure and identify immediate issues
-- This helps us plan our cleaning strategy

-- 2.1 Sample Data Inspection
SELECT * 
FROM raw_data.life_expectancy 
LIMIT 20;

-- 2.2 Data Type Verification
SELECT 
    column_name,
    data_type,
    is_nullable,
    character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'raw_data' 
    AND table_name = 'life_expectancy'
ORDER BY ordinal_position;

-- 2.3 Year Range Analysis
SELECT 
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT year) as total_years,
    string_agg(DISTINCT year::text, ', ' ORDER BY year::text) as all_years
FROM raw_data.life_expectancy;

-- 2.4 Country Count by Year
-- Why: Identifies if certain years have missing countries
SELECT 
    year,
    COUNT(DISTINCT country) as country_count,
    193 - COUNT(DISTINCT country) as missing_countries
FROM raw_data.life_expectancy
GROUP BY year
ORDER BY year;

-- 2.5 Development Status Distribution
SELECT 
    status,
    COUNT(DISTINCT country) as country_count,
    COUNT(*) as total_records,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM raw_data.life_expectancy
GROUP BY status;

-- =====================================================
-- SECTION 3: DATA QUALITY ASSESSMENT
-- =====================================================
-- Purpose: Systematic evaluation of data quality issues
-- This is crucial for ensuring reliable analysis results

-- 3.1 Missing Values Analysis by Column
WITH missing_counts AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(*) - COUNT(country) as missing_country,
        COUNT(*) - COUNT(year) as missing_year,
        COUNT(*) - COUNT(status) as missing_status,
        COUNT(*) - COUNT(life_expectancy) as missing_life_expectancy,
        COUNT(*) - COUNT(adult_mortality) as missing_adult_mortality,
        COUNT(*) - COUNT(infant_deaths) as missing_infant_deaths,
        COUNT(*) - COUNT(alcohol) as missing_alcohol,
        COUNT(*) - COUNT(percentage_expenditure) as missing_percentage_expenditure,
        COUNT(*) - COUNT(hepatitis_b) as missing_hepatitis_b,
        COUNT(*) - COUNT(measles) as missing_measles,
        COUNT(*) - COUNT(bmi) as missing_bmi,
        COUNT(*) - COUNT(under_five_deaths) as missing_under_five_deaths,
        COUNT(*) - COUNT(polio) as missing_polio,
        COUNT(*) - COUNT(total_expenditure) as missing_total_expenditure,
        COUNT(*) - COUNT(diphtheria) as missing_diphtheria,
        COUNT(*) - COUNT(hiv_aids) as missing_hiv_aids,
        COUNT(*) - COUNT(gdp) as missing_gdp,
        COUNT(*) - COUNT(population) as missing_population,
        COUNT(*) - COUNT(thinness_1_19_years) as missing_thinness_1_19,
        COUNT(*) - COUNT(thinness_5_9_years) as missing_thinness_5_9,
        COUNT(*) - COUNT(income_composition_of_resources) as missing_income_composition,
        COUNT(*) - COUNT(schooling) as missing_schooling
    FROM raw_data.life_expectancy
)
SELECT 
    column_name,
    missing_count,
    ROUND(100.0 * missing_count / total_records, 2) as missing_percentage,
    CASE 
        WHEN missing_count = 0 THEN 'Complete'
        WHEN missing_count < total_records * 0.05 THEN 'Good (<5% missing)'
        WHEN missing_count < total_records * 0.20 THEN 'Moderate (5-20% missing)'
        ELSE 'Poor (>20% missing)'
    END as data_quality
FROM (
    SELECT total_records, 'country' as column_name, missing_country as missing_count FROM missing_counts
    UNION ALL SELECT total_records, 'year', missing_year FROM missing_counts
    UNION ALL SELECT total_records, 'status', missing_status FROM missing_counts
    UNION ALL SELECT total_records, 'life_expectancy', missing_life_expectancy FROM missing_counts
    UNION ALL SELECT total_records, 'adult_mortality', missing_adult_mortality FROM missing_counts
    UNION ALL SELECT total_records, 'infant_deaths', missing_infant_deaths FROM missing_counts
    UNION ALL SELECT total_records, 'alcohol', missing_alcohol FROM missing_counts
    UNION ALL SELECT total_records, 'percentage_expenditure', missing_percentage_expenditure FROM missing_counts
    UNION ALL SELECT total_records, 'hepatitis_b', missing_hepatitis_b FROM missing_counts
    UNION ALL SELECT total_records, 'measles', missing_measles FROM missing_counts
    UNION ALL SELECT total_records, 'bmi', missing_bmi FROM missing_counts
    UNION ALL SELECT total_records, 'under_five_deaths', missing_under_five_deaths FROM missing_counts
    UNION ALL SELECT total_records, 'polio', missing_polio FROM missing_counts
    UNION ALL SELECT total_records, 'total_expenditure', missing_total_expenditure FROM missing_counts
    UNION ALL SELECT total_records, 'diphtheria', missing_diphtheria FROM missing_counts
    UNION ALL SELECT total_records, 'hiv_aids', missing_hiv_aids FROM missing_counts
    UNION ALL SELECT total_records, 'gdp', missing_gdp FROM missing_counts
    UNION ALL SELECT total_records, 'population', missing_population FROM missing_counts
    UNION ALL SELECT total_records, 'thinness_1_19_years', missing_thinness_1_19 FROM missing_counts
    UNION ALL SELECT total_records, 'thinness_5_9_years', missing_thinness_5_9 FROM missing_counts
    UNION ALL SELECT total_records, 'income_composition_of_resources', missing_income_composition FROM missing_counts
    UNION ALL SELECT total_records, 'schooling', missing_schooling FROM missing_counts
) t
ORDER BY missing_percentage DESC;

-- 3.2 Outlier Detection - Impossible Values
-- Why: Biological and logical constraints help identify data errors
WITH outliers AS (
    SELECT 
        'Life Expectancy > 90' as issue_type,
        COUNT(*) as count,
        string_agg(DISTINCT country || ' (' || year || '): ' || life_expectancy, '; ' LIMIT 5) as examples
    FROM raw_data.life_expectancy
    WHERE life_expectancy > 90
    
    UNION ALL
    
    SELECT 
        'Life Expectancy < 35',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || '): ' || life_expectancy, '; ' LIMIT 5)
    FROM raw_data.life_expectancy
    WHERE life_expectancy < 35 AND life_expectancy IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Adult Mortality > 700',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || '): ' || adult_mortality, '; ' LIMIT 5)
    FROM raw_data.life_expectancy
    WHERE adult_mortality > 700
    
    UNION ALL
    
    SELECT 
        'BMI > 60',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || '): ' || bmi, '; ' LIMIT 5)
    FROM raw_data.life_expectancy
    WHERE bmi > 60
    
    UNION ALL
    
    SELECT 
        'BMI < 15',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || '): ' || bmi, '; ' LIMIT 5)
    FROM raw_data.life_expectancy
    WHERE bmi < 15 AND bmi IS NOT NULL
    
    UNION ALL
    
    SELECT 
        'Immunization Coverage > 100%',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || '): hep_b=' || hepatitis_b || ', polio=' || polio || ', diph=' || diphtheria, '; ' LIMIT 3)
    FROM raw_data.life_expectancy
    WHERE hepatitis_b > 100 OR polio > 100 OR diphtheria > 100
    
    UNION ALL
    
    SELECT 
        'Zero Population',
        COUNT(*),
        string_agg(DISTINCT country || ' (' || year || ')', '; ' LIMIT 5)
    FROM raw_data.life_expectancy
    WHERE population = 0 OR population IS NULL
)
SELECT * FROM outliers
WHERE count > 0
ORDER BY count DESC;

-- 3.3 Duplicate Check
-- Why: Duplicates can skew analysis results
SELECT 
    country,
    year,
    COUNT(*) as duplicate_count
FROM raw_data.life_expectancy
GROUP BY country, year
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, country, year;

-- 3.4 Country Name Inconsistencies
-- Why: Same country might be recorded differently
SELECT 
    country,
    COUNT(*) as record_count,
    COUNT(DISTINCT year) as years_present,
    MIN(year) as first_year,
    MAX(year) as last_year,
    CASE 
        WHEN COUNT(DISTINCT year) < 16 THEN 'Incomplete Timeline'
        ELSE 'Complete Timeline'
    END as timeline_status
FROM raw_data.life_expectancy
GROUP BY country
ORDER BY 
    CASE 
        WHEN COUNT(DISTINCT year) < 16 THEN 0 
        ELSE 1 
    END,
    country;

-- =====================================================
-- SECTION 4: DATA CLEANING AND STANDARDIZATION
-- =====================================================
-- Purpose: Create a clean dataset with consistent standards
-- This is the foundation for all subsequent analysis

-- 4.1 Create Country Mapping Table
-- Why: Standardize country names to match international databases
DROP TABLE IF EXISTS cleaned_data.country_mapping CASCADE;
CREATE TABLE cleaned_data.country_mapping AS
SELECT DISTINCT
    country as original_name,
    CASE 
        -- Standardize to match World Bank, UN, and WHO naming conventions
        WHEN country = 'Bolivia' THEN 'Bolivia (Plurinational State of)'
        WHEN country = 'Brunei' THEN 'Brunei Darussalam'
        WHEN country = 'Cape Verde' THEN 'Cabo Verde'
        WHEN country = 'Congo' AND EXISTS (
            SELECT 1 FROM raw_data.life_expectancy 
            WHERE country = 'Democratic Republic of Congo'
        ) THEN 'Congo'
        WHEN country = 'Democratic Republic of Congo' THEN 'Congo (Democratic Republic of the)'
        WHEN country = 'Democratic People''s Republic of Korea' THEN 'Korea (Democratic People''s Republic of)'
        WHEN country = 'Republic of Korea' THEN 'Korea (Republic of)'
        WHEN country = 'Côte d''Ivoire' THEN 'Côte d''Ivoire'
        WHEN country = 'Czech Republic' THEN 'Czechia'
        WHEN country = 'Iran' THEN 'Iran (Islamic Republic of)'
        WHEN country LIKE '%Iran%' THEN 'Iran (Islamic Republic of)'
        WHEN country = 'Lao People''s Democratic Republic' THEN 'Lao People''s Democratic Republic'
        WHEN country = 'Laos' THEN 'Lao People''s Democratic Republic'
        WHEN country = 'Micronesia' THEN 'Micronesia (Federated States of)'
        WHEN country = 'Micronesia (Federated States of)' THEN 'Micronesia (Federated States of)'
        WHEN country = 'Moldova' THEN 'Moldova (Republic of)'
        WHEN country = 'Republic of Moldova' THEN 'Moldova (Republic of)'
        WHEN country = 'Macedonia' THEN 'North Macedonia'
        WHEN country = 'The former Yugoslav republic of Macedonia' THEN 'North Macedonia'
        WHEN country = 'Palestine' THEN 'Palestine, State of'
        WHEN country = 'Russian Federation' THEN 'Russian Federation'
        WHEN country = 'Russia' THEN 'Russian Federation'
        WHEN country = 'Syria' THEN 'Syrian Arab Republic'
        WHEN country = 'Syrian Arab Republic' THEN 'Syrian Arab Republic'
        WHEN country = 'United Republic of Tanzania' THEN 'Tanzania (United Republic of)'
        WHEN country = 'Tanzania' THEN 'Tanzania (United Republic of)'
        WHEN country = 'Timor-Leste' THEN 'Timor-Leste'
        WHEN country = 'East Timor' THEN 'Timor-Leste'
        WHEN country = 'United Kingdom' THEN 'United Kingdom'
        WHEN country = 'United Kingdom of Great Britain and Northern Ireland' THEN 'United Kingdom'
        WHEN country = 'United States' THEN 'United States of America'
        WHEN country = 'United States of America' THEN 'United States of America'
        WHEN country = 'USA' THEN 'United States of America'
        WHEN country = 'Venezuela' THEN 'Venezuela (Bolivarian Republic of)'
        WHEN country = 'Venezuela (Bolivarian Republic of)' THEN 'Venezuela (Bolivarian Republic of)'
        WHEN country = 'Viet Nam' THEN 'Viet Nam'
        WHEN country = 'Vietnam' THEN 'Viet Nam'
        ELSE country
    END as standardized_name,
    -- Add ISO codes for future joins
    CASE 
        WHEN country IN ('United States', 'USA', 'United States of America') THEN 'USA'
        WHEN country IN ('United Kingdom', 'United Kingdom of Great Britain and Northern Ireland') THEN 'GBR'
        WHEN country = 'Germany' THEN 'DEU'
        WHEN country = 'France' THEN 'FRA'
        WHEN country = 'China' THEN 'CHN'
        WHEN country = 'India' THEN 'IND'
        WHEN country = 'Brazil' THEN 'BRA'
        WHEN country = 'Russian Federation' THEN 'RUS'
        WHEN country = 'Japan' THEN 'JPN'
        WHEN country = 'Mexico' THEN 'MEX'
        -- Add more as needed
        ELSE NULL
    END as iso_code_3
FROM raw_data.life_expectancy;

-- 4.2 Create Cleaned Main Table
DROP TABLE IF EXISTS cleaned_data.life_expectancy CASCADE;
CREATE TABLE cleaned_data.life_expectancy AS
WITH cleaned_records AS (
    SELECT 
        cm.standardized_name as country,
        cm.iso_code_3,
        r.year,
        r.status,
        
        -- Clean life expectancy with biological constraints
        CASE 
            WHEN r.life_expectancy > 100 THEN NULL  -- Impossible
            WHEN r.life_expectancy < 20 THEN NULL   -- Highly unlikely
            WHEN r.life_expectancy > 90 THEN 90     -- Cap at reasonable maximum
            ELSE r.life_expectancy
        END as life_expectancy,
        
        -- Clean adult mortality (per 1000 population aged 15-60)
        CASE 
            WHEN r.adult_mortality > 1000 THEN NULL  -- Rate cannot exceed 1000 per 1000
            WHEN r.adult_mortality < 0 THEN NULL
            WHEN r.adult_mortality > 700 THEN 700    -- Cap at reasonable maximum
            ELSE r.adult_mortality
        END as adult_mortality,
        
        -- Clean infant deaths (absolute numbers)
        CASE 
            WHEN r.infant_deaths < 0 THEN 0
            WHEN r.population > 0 AND r.infant_deaths > r.population * 0.1 THEN NULL  -- Sanity check
            ELSE r.infant_deaths
        END as infant_deaths,
        
        -- Clean alcohol consumption (litres per capita)
        CASE 
            WHEN r.alcohol < 0 THEN 0
            WHEN r.alcohol > 25 THEN NULL  -- Extremely high, likely error
            ELSE r.alcohol
        END as alcohol,
        
        -- Clean percentage expenditure (relative to GDP per capita)
        CASE 
            WHEN r.percentage_expenditure < 0 THEN 0
            ELSE r.percentage_expenditure
        END as percentage_expenditure,
        
        -- Clean immunization coverage (percentages, cap at 100)
        CASE 
            WHEN r.hepatitis_b > 100 THEN 99  -- Cap at 99 to indicate high coverage
            WHEN r.hepatitis_b < 0 THEN NULL
            ELSE r.hepatitis_b
        END as hepatitis_b,
        
        -- Clean measles cases (absolute numbers)
        CASE 
            WHEN r.measles < 0 THEN 0
            ELSE r.measles
        END as measles,
        
        -- Clean BMI with physiological constraints
        CASE 
            WHEN r.bmi < 10 THEN NULL  -- Incompatible with life
            WHEN r.bmi > 60 THEN NULL  -- Extremely unlikely
            ELSE r.bmi
        END as bmi,
        
        -- Clean under-five deaths
        CASE 
            WHEN r.under_five_deaths < 0 THEN 0
            WHEN r.population > 0 AND r.under_five_deaths > r.population * 0.2 THEN NULL
            ELSE r.under_five_deaths
        END as under_five_deaths,
        
        -- Clean polio immunization
        CASE 
            WHEN r.polio > 100 THEN 99
            WHEN r.polio < 0 THEN NULL
            ELSE r.polio
        END as polio,
        
        -- Clean total health expenditure (% of GDP)
        CASE 
            WHEN r.total_expenditure < 0 THEN NULL
            WHEN r.total_expenditure > 20 THEN NULL  -- Very few countries exceed this
            ELSE r.total_expenditure
        END as total_expenditure,
        
        -- Clean diphtheria immunization
        CASE 
            WHEN r.diphtheria > 100 THEN 99
            WHEN r.diphtheria < 0 THEN NULL
            ELSE r.diphtheria
        END as diphtheria,
        
        -- Clean HIV/AIDS deaths (per 1000 live births)
        CASE 
            WHEN r.hiv_aids < 0 THEN 0
            WHEN r.hiv_aids > 50 THEN NULL  -- Sanity check
            ELSE r.hiv_aids
        END as hiv_aids,
        
        -- Clean GDP
        CASE 
            WHEN r.gdp < 0 THEN NULL
            WHEN r.gdp > 200000 THEN NULL  -- Per capita GDP sanity check
            ELSE r.gdp
        END as gdp,
        
        -- Clean population
        CASE 
            WHEN r.population <= 0 THEN NULL
            WHEN r.population > 2000000000 THEN NULL  -- Sanity check (only China and India)
            ELSE r.population
        END as population,
        
        -- Clean thinness prevalence
        CASE 
            WHEN r.thinness_1_19_years < 0 THEN NULL
            WHEN r.thinness_1_19_years > 50 THEN NULL  -- Unlikely prevalence
            ELSE r.thinness_1_19_years
        END as thinness_1_19_years,
        
        CASE 
            WHEN r.thinness_5_9_years < 0 THEN NULL
            WHEN r.thinness_5_9_years > 50 THEN NULL
            ELSE r.thinness_5_9_years
        END as thinness_5_9_years,
        
        -- Clean income composition (HDI component, 0-1 scale)
        CASE 
            WHEN r.income_composition_of_resources < 0 THEN 0
            WHEN r.income_composition_of_resources > 1 THEN 1
            ELSE r.income_composition_of_resources
        END as income_composition_of_resources,
        
        -- Clean schooling years
        CASE 
            WHEN r.schooling < 0 THEN NULL
            WHEN r.schooling > 25 THEN NULL  -- Unlikely average
            ELSE r.schooling
        END as schooling,
        
        -- Add data quality flag
        CURRENT_TIMESTAMP as cleaned_date,
        'Initial cleaning - biological constraints applied' as cleaning_notes
        
    FROM raw_data.life_expectancy r
    JOIN cleaned_data.country_mapping cm ON r.country = cm.original_name
)
SELECT * FROM cleaned_records;

-- 4.3 Add calculated fields
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS gdp_per_capita NUMERIC;
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS health_expenditure_per_capita NUMERIC;
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS infant_mortality_rate NUMERIC;
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS under_five_mortality_rate NUMERIC;
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS immunization_coverage_avg NUMERIC;

UPDATE cleaned_data.life_expectancy
SET 
    -- Calculate per capita metrics
    gdp_per_capita = CASE 
        WHEN population > 0 AND gdp IS NOT NULL 
        THEN gdp / population::numeric
        ELSE NULL 
    END,
    
    health_expenditure_per_capita = CASE 
        WHEN population > 0 AND gdp IS NOT NULL AND total_expenditure IS NOT NULL
        THEN (gdp / population::numeric) * (total_expenditure / 100)
        ELSE NULL 
    END,
    
    -- Calculate mortality rates per 1,000 live births
    infant_mortality_rate = CASE 
        WHEN population > 0 AND infant_deaths IS NOT NULL 
        THEN (infant_deaths::numeric * 1000) / population
        ELSE NULL 
    END,
    
    under_five_mortality_rate = CASE 
        WHEN population > 0 AND under_five_deaths IS NOT NULL 
        THEN (under_five_deaths::numeric * 1000) / population
        ELSE NULL 
    END,
    
    -- Calculate average immunization coverage
    immunization_coverage_avg = (
        COALESCE(hepatitis_b, 0) + 
        COALESCE(polio, 0) + 
        COALESCE(diphtheria, 0)
    ) / NULLIF(
        (CASE WHEN hepatitis_b IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN polio IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN diphtheria IS NOT NULL THEN 1 ELSE 0 END), 0
    );

-- 4.4 Create data quality scores
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS completeness_score NUMERIC;
ALTER TABLE cleaned_data.life_expectancy ADD COLUMN IF NOT EXISTS quality_score INTEGER;

WITH field_counts AS (
    SELECT 
        country,
        year,
        -- Count non-null fields (excluding calculated fields)
        (CASE WHEN life_expectancy IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN adult_mortality IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN infant_deaths IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN alcohol IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN percentage_expenditure IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN hepatitis_b IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN measles IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN bmi IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN under_five_deaths IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN polio IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN total_expenditure IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN diphtheria IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN hiv_aids IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN gdp IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN population IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN thinness_1_19_years IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN thinness_5_9_years IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN income_composition_of_resources IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN schooling IS NOT NULL THEN 1 ELSE 0 END) as non_null_count
    FROM cleaned_data.life_expectancy
)
UPDATE cleaned_data.life_expectancy le
SET 
    completeness_score = ROUND(fc.non_null_count * 100.0 / 19, 2),
    quality_score = CASE 
        WHEN fc.non_null_count >= 17 AND le.life_expectancy IS NOT NULL THEN 100
        WHEN fc.non_null_count >= 15 AND le.life_expectancy IS NOT NULL THEN 80
        WHEN fc.non_null_count >= 12 AND le.life_expectancy IS NOT NULL THEN 60
        WHEN fc.non_null_count >= 10 THEN 40
        ELSE 20
    END
FROM field_counts fc
WHERE le.country = fc.country AND le.year = fc.year;

-- 4.5 Verify cleaning results
SELECT 
    'Original Records' as dataset,
    COUNT(*) as record_count,
    COUNT(DISTINCT country) as countries,
    COUNT(DISTINCT year) as years
FROM raw_data.life_expectancy
UNION ALL
SELECT 
    'Cleaned Records',
    COUNT(*),
    COUNT(DISTINCT country),
    COUNT(DISTINCT year)
FROM cleaned_data.life_expectancy;

-- =====================================================
-- SECTION 5: EXTERNAL DATA INTEGRATION AND VALIDATION
-- =====================================================
-- Purpose: Validate our data against authoritative external sources
-- This ensures accuracy and adds important context

-- 5.1 Create external reference tables
-- Note: In practice, you would import these from World Bank, IMF, WHO APIs or files

-- World Bank Country Classifications
DROP TABLE IF EXISTS external_data.world_bank_countries CASCADE;
CREATE TABLE external_data.world_bank_countries (
    country_name VARCHAR(255),
    country_code VARCHAR(3),
    region VARCHAR(100),
    income_group VARCHAR(50),
    lending_category VARCHAR(50),
    population_2015 BIGINT,
    gdp_2015_usd NUMERIC,
    gni_per_capita_2015 NUMERIC
);

-- Sample data (you would load full dataset)
INSERT INTO external_data.world_bank_countries 
(country_name, country_code, region, income_group, population_2015, gdp_2015_usd) 
VALUES 
('United States of America', 'USA', 'North America', 'High income', 321418820, 18036648000000),
('China', 'CHN', 'East Asia & Pacific', 'Upper middle income', 1371220000, 11015542000000),
('India', 'IND', 'South Asia', 'Lower middle income', 1310152403, 2102391000000),
('Brazil', 'BRA', 'Latin America & Caribbean', 'Upper middle income', 205962108, 1800046000000),
('Germany', 'DEU', 'Europe & Central Asia', 'High income', 81686611, 3363447000000);

-- WHO Health Statistics
DROP TABLE IF EXISTS external_data.who_health_stats CASCADE;
CREATE TABLE external_data.who_health_stats (
    country_name VARCHAR(255),
    year INTEGER,
    life_expectancy_who NUMERIC,
    infant_mortality_who NUMERIC,
    maternal_mortality_who NUMERIC,
    physicians_per_1000 NUMERIC,
    hospital_beds_per_1000 NUMERIC
);

-- IMF Economic Data
DROP TABLE IF EXISTS external_data.imf_economic_data CASCADE;
CREATE TABLE external_data.imf_economic_data (
    country_name VARCHAR(255),
    year INTEGER,
    gdp_current_usd NUMERIC,
    gdp_per_capita_current_usd NUMERIC,
    inflation_rate NUMERIC,
    unemployment_rate NUMERIC
);

-- 5.2 Create validation summary
CREATE OR REPLACE VIEW analytics.data_validation_summary AS
WITH validation_checks AS (
    SELECT 
        c.country,
        c.year,
        c.population as our_population,
        wb.population_2015 as wb_population,
        CASE 
            WHEN c.year = 2015 AND wb.population_2015 IS NOT NULL 
            THEN ABS(c.population - wb.population_2015) * 100.0 / NULLIF(wb.population_2015, 0)
            ELSE NULL
        END as population_diff_pct,
        
        c.gdp as our_gdp,
        wb.gdp_2015_usd as wb_gdp,
        c.life_expectancy as our_life_exp,
        who.life_expectancy_who as who_life_exp
        
    FROM cleaned_data.life_expectancy c
    LEFT JOIN external_data.world_bank_countries wb 
        ON c.country = wb.country_name
    LEFT JOIN external_data.who_health_stats who 
        ON c.country = who.country_name AND c.year = who.year
    WHERE c.year = 2015
)
SELECT 
    country,
    ROUND(population_diff_pct, 2) as population_variance_pct,
    CASE 
        WHEN population_diff_pct IS NULL THEN 'No External Data'
        WHEN population_diff_pct < 5 THEN 'Validated'
        WHEN population_diff_pct < 10 THEN 'Minor Variance'
        ELSE 'Major Variance - Investigate'
    END as validation_status,
    our_population,
    wb_population,
    our_life_exp,
    who_life_exp
FROM validation_checks
ORDER BY population_diff_pct DESC NULLS LAST;

-- 5.3 Add external classifications to our data
ALTER TABLE cleaned_data.life_expectancy 
ADD COLUMN IF NOT EXISTS region VARCHAR(100),
ADD COLUMN IF NOT EXISTS income_group VARCHAR(50),
ADD COLUMN IF NOT EXISTS is_oecd BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS is_g20 BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS is_g7 BOOLEAN DEFAULT FALSE;

-- Update with World Bank classifications
UPDATE cleaned_data.life_expectancy le
SET 
    region = wb.region,
    income_group = wb.income_group
FROM external_data.world_bank_countries wb
WHERE le.country = wb.country_name;

-- Mark special country groups
UPDATE cleaned_data.life_expectancy
SET is_g7 = TRUE
WHERE country IN (
    'United States of America', 'United Kingdom', 'France', 
    'Germany', 'Italy', 'Canada', 'Japan'
);

UPDATE cleaned_data.life_expectancy
SET is_g20 = TRUE
WHERE country IN (
    'United States of America', 'United Kingdom', 'France', 'Germany', 
    'Italy', 'Canada', 'Japan', 'Russian Federation', 'China', 'India', 
    'Brazil', 'Mexico', 'South Africa', 'Australia', 'Korea (Republic of)', 
    'Indonesia', 'Saudi Arabia', 'Turkey', 'Argentina'
);

-- =====================================================
-- SECTION 6: ADVANCED FEATURE ENGINEERING
-- =====================================================
-- Purpose: Create derived features that provide deeper insights

-- 6.1 Create time-based features
ALTER TABLE cleaned_data.life_expectancy
ADD COLUMN IF NOT EXISTS life_exp_change_1yr NUMERIC,
ADD COLUMN IF NOT EXISTS life_exp_change_5yr NUMERIC,
ADD COLUMN IF NOT EXISTS gdp_growth_rate NUMERIC,
ADD COLUMN IF NOT EXISTS health_spending_efficiency NUMERIC;

-- Calculate year-over-year changes
WITH lagged_data AS (
    SELECT 
        country,
        year,
        life_expectancy,
        LAG(life_expectancy, 1) OVER (PARTITION BY country ORDER BY year) as prev_year_life_exp,
        LAG(life_expectancy, 5) OVER (PARTITION BY country ORDER BY year) as five_year_ago_life_exp,
        gdp_per_capita,
        LAG(gdp_per_capita, 1) OVER (PARTITION BY country ORDER BY year) as prev_year_gdp
    FROM cleaned_data.life_expectancy
)
UPDATE cleaned_data.life_expectancy le
SET 
    life_exp_change_1yr = ld.life_expectancy - ld.prev_year_life_exp,
    life_exp_change_5yr = ld.life_expectancy - ld.five_year_ago_life_exp,
    gdp_growth_rate = CASE 
        WHEN ld.prev_year_gdp > 0 
        THEN ((ld.gdp_per_capita - ld.prev_year_gdp) / ld.prev_year_gdp) * 100
        ELSE NULL
    END
FROM lagged_data ld
WHERE le.country = ld.country AND le.year = ld.year;

-- Calculate health spending efficiency
UPDATE cleaned_data.life_expectancy
SET health_spending_efficiency = CASE 
    WHEN total_expenditure > 0 
    THEN life_expectancy / total_expenditure
    ELSE NULL
END;

-- 6.2 Create composite health indicators
ALTER TABLE cleaned_data.life_expectancy
ADD COLUMN IF NOT EXISTS health_system_performance_score NUMERIC,
ADD COLUMN IF NOT EXISTS disease_burden_score NUMERIC,
ADD COLUMN IF NOT EXISTS social_determinants_score NUMERIC;

-- Health system performance (0-100 scale)
UPDATE cleaned_data.life_expectancy
SET health_system_performance_score = (
    -- Immunization coverage (40% weight)
    COALESCE(immunization_coverage_avg, 0) * 0.4 +
    -- Inverse infant mortality (30% weight)
    CASE 
        WHEN infant_mortality_rate IS NOT NULL AND infant_mortality_rate > 0
        THEN (1 - LEAST(infant_mortality_rate / 100, 1)) * 100 * 0.3
        ELSE 50 * 0.3  -- Default to middle value
    END +
    -- Health spending efficiency (30% weight)
    CASE 
        WHEN health_spending_efficiency IS NOT NULL
        THEN LEAST(health_spending_efficiency / 20 * 100, 100) * 0.3
        ELSE 50 * 0.3
    END
);

-- Disease burden score (lower is worse)
UPDATE cleaned_data.life_expectancy
SET disease_burden_score = 100 - (
    -- HIV/AIDS impact (40% weight)
    LEAST(COALESCE(hiv_aids, 0) * 2, 40) +
    -- Adult mortality (40% weight)
    LEAST(COALESCE(adult_mortality, 0) / 10, 40) +
    -- Communicable disease (measles as proxy, 20% weight)
    CASE 
        WHEN population > 0 AND measles IS NOT NULL
        THEN LEAST((measles::numeric / population) * 100000, 20)
        ELSE 10
    END
);

-- Social determinants score
UPDATE cleaned_data.life_expectancy
SET social_determinants_score = (
    -- Education (40% weight)
    LEAST(COALESCE(schooling, 0) * 5, 40) +
    -- Income composition (40% weight)
    COALESCE(income_composition_of_resources, 0.5) * 100 * 0.4 +
    -- Nutrition (BMI as proxy, 20% weight)
    CASE 
        WHEN bmi BETWEEN 18.5 AND 30 THEN 20
        WHEN bmi BETWEEN 16 AND 35 THEN 10
        ELSE 0
    END
);

-- =====================================================
-- SECTION 7: ANALYTICAL INSIGHTS - COMPREHENSIVE ANALYSIS
-- =====================================================
-- Purpose: Extract meaningful insights for decision-making

-- 7.1 Global Trends Overview
CREATE OR REPLACE VIEW analytics.global_trends AS
WITH yearly_stats AS (
    SELECT 
        year,
        status,
        COUNT(DISTINCT country) as countries,
        ROUND(AVG(life_expectancy), 2) as avg_life_expectancy,
        ROUND(STDDEV(life_expectancy), 2) as std_dev,
        ROUND(MIN(life_expectancy), 2) as min_life_exp,
        ROUND(MAX(life_expectancy), 2) as max_life_exp,
        ROUND(AVG(gdp_per_capita), 0) as avg_gdp_per_capita,
        ROUND(AVG(total_expenditure), 2) as avg_health_spending_pct
    FROM cleaned_data.life_expectancy
    WHERE quality_score >= 60
    GROUP BY year, status
),
gap_analysis AS (
    SELECT 
        year,
        MAX(CASE WHEN status = 'Developed' THEN avg_life_expectancy END) -
        MAX(CASE WHEN status = 'Developing' THEN avg_life_expectancy END) as development_gap
    FROM yearly_stats
    GROUP BY year
)
SELECT 
    ys.*,
    ga.development_gap,
    ROUND(
        100.0 * (ys.avg_life_expectancy - FIRST_VALUE(ys.avg_life_expectancy) 
        OVER (PARTITION BY ys.status ORDER BY ys.year)) / 
        FIRST_VALUE(ys.avg_life_expectancy) OVER (PARTITION BY ys.status ORDER BY ys.year), 
        2
    ) as pct_change_since_2000
FROM yearly_stats ys
JOIN gap_analysis ga ON ys.year = ga.year
ORDER BY year DESC, status;

-- 7.2 Country Success Stories
CREATE OR REPLACE VIEW analytics.country_improvements AS
WITH country_changes AS (
    SELECT 
        country,
        MAX(CASE WHEN year = 2000 THEN life_expectancy END) as life_exp_2000,
        MAX(CASE WHEN year = 2015 THEN life_expectancy END) as life_exp_2015,
        MAX(CASE WHEN year = 2000 THEN gdp_per_capita END) as gdp_2000,
        MAX(CASE WHEN year = 2015 THEN gdp_per_capita END) as gdp_2015,
        MAX(CASE WHEN year = 2000 THEN hiv_aids END) as hiv_2000,
        MAX(CASE WHEN year = 2015 THEN hiv_aids END) as hiv_2015,
        MAX(CASE WHEN year = 2015 THEN income_group END) as income_group,
        MAX(CASE WHEN year = 2015 THEN region END) as region
    FROM cleaned_data.life_expectancy
    WHERE year IN (2000, 2015) AND quality_score >= 60
    GROUP BY country
    HAVING COUNT(DISTINCT year) = 2
)
SELECT 
    country,
    income_group,
    region,
    ROUND(life_exp_2000, 1) as life_expectancy_2000,
    ROUND(life_exp_2015, 1) as life_expectancy_2015,
    ROUND(life_exp_2015 - life_exp_2000, 1) as improvement_years,
    ROUND((life_exp_2015 - life_exp_2000) / life_exp_2000 * 100, 2) as improvement_pct,
    ROUND(gdp_2000, 0) as gdp_per_capita_2000,
    ROUND(gdp_2015, 0) as gdp_per_capita_2015,
    CASE 
        WHEN hiv_2000 > 10 AND hiv_2015 < 5 THEN 'Major HIV Success'
        WHEN life_exp_2015 - life_exp_2000 > 15 THEN 'Exceptional Progress'
        WHEN life_exp_2015 - life_exp_2000 > 10 THEN 'Strong Progress'
        WHEN life_exp_2015 - life_exp_2000 > 5 THEN 'Good Progress'
        WHEN life_exp_2015 - life_exp_2000 > 0 THEN 'Modest Progress'
        ELSE 'Declining'
    END as progress_category
FROM country_changes
WHERE life_exp_2000 IS NOT NULL AND life_exp_2015 IS NOT NULL
ORDER BY improvement_years DESC;

-- 7.3 Healthcare Efficiency Analysis
CREATE OR REPLACE VIEW analytics.healthcare_efficiency AS
WITH efficiency_metrics AS (
    SELECT 
        country,
        year,
        life_expectancy,
        total_expenditure as health_spending_pct,
        health_spending_efficiency,
        gdp_per_capita,
        income_group,
        RANK() OVER (PARTITION BY year ORDER BY health_spending_efficiency DESC) as efficiency_rank
    FROM cleaned_data.life_expectancy
    WHERE year >= 2010 
        AND total_expenditure > 0 
        AND life_expectancy IS NOT NULL
        AND quality_score >= 80
)
SELECT 
    country,
    income_group,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(AVG(health_spending_pct), 2) as avg_health_spending_pct,
    ROUND(AVG(health_spending_efficiency), 2) as avg_efficiency_score,
    ROUND(AVG(gdp_per_capita), 0) as avg_gdp_per_capita,
    ROUND(AVG(efficiency_rank), 0) as avg_efficiency_rank,
    COUNT(*) as years_of_data,
    CASE 
        WHEN AVG(health_spending_efficiency) > 15 THEN 'Very Efficient'
        WHEN AVG(health_spending_efficiency) > 10 THEN 'Efficient'
        WHEN AVG(health_spending_efficiency) > 7 THEN 'Moderate'
        ELSE 'Low Efficiency'
    END as efficiency_category
FROM efficiency_metrics
GROUP BY country, income_group
HAVING COUNT(*) >= 3
ORDER BY avg_efficiency_score DESC;

-- 7.4 Key Determinants Analysis
CREATE OR REPLACE VIEW analytics.life_expectancy_correlations AS
WITH correlation_data AS (
    SELECT 
        life_expectancy,
        adult_mortality,
        infant_mortality_rate,
        alcohol,
        percentage_expenditure,
        immunization_coverage_avg,
        bmi,
        under_five_mortality_rate,
        hiv_aids,
        gdp_per_capita,
        income_composition_of_resources,
        schooling
    FROM cleaned_data.life_expectancy
    WHERE quality_score >= 80
)
SELECT 
    'Income Composition Index' as factor,
    ROUND(CORR(life_expectancy, income_composition_of_resources)::numeric, 4) as correlation_coefficient,
    'Composite development measure' as description
FROM correlation_data
UNION ALL
SELECT 
    'Years of Schooling',
    ROUND(CORR(life_expectancy, schooling)::numeric, 4),
    'Average years of education'
FROM correlation_data
UNION ALL
SELECT 
    'Adult Mortality',
    ROUND(CORR(life_expectancy, adult_mortality)::numeric, 4),
    'Deaths per 1000 adults aged 15-60'
FROM correlation_data
UNION ALL
SELECT 
    'HIV/AIDS',
    ROUND(CORR(life_expectancy, hiv_aids)::numeric, 4),
    'Deaths per 1000 live births'
FROM correlation_data
UNION ALL
SELECT 
    'GDP Per Capita',
    ROUND(CORR(life_expectancy, gdp_per_capita)::numeric, 4),
    'Economic prosperity measure'
FROM correlation_data
UNION ALL
SELECT 
    'Immunization Coverage',
    ROUND(CORR(life_expectancy, immunization_coverage_avg)::numeric, 4),
    'Average of HepB, Polio, DPT coverage'
FROM correlation_data
ORDER BY ABS(correlation_coefficient) DESC;

-- 7.5 Disease Impact Analysis
CREATE OR REPLACE VIEW analytics.disease_burden_analysis AS
SELECT 
    CASE 
        WHEN hiv_aids < 0.1 THEN '1. Minimal (<0.1)'
        WHEN hiv_aids < 0.5 THEN '2. Very Low (0.1-0.5)'
        WHEN hiv_aids < 2 THEN '3. Low (0.5-2)'
        WHEN hiv_aids < 5 THEN '4. Moderate (2-5)'
        WHEN hiv_aids < 10 THEN '5. High (5-10)'
        ELSE '6. Very High (>10)'
    END as hiv_burden_category,
    COUNT(DISTINCT country) as countries,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(AVG(adult_mortality), 0) as avg_adult_mortality,
    ROUND(AVG(infant_mortality_rate), 2) as avg_infant_mortality,
    ROUND(MIN(life_expectancy), 1) as min_life_expectancy,
    ROUND(MAX(life_expectancy), 1) as max_life_expectancy,
    STRING_AGG(
        CASE WHEN hiv_aids > 10 THEN country END, 
        ', ' ORDER BY hiv_aids DESC
    ) as most_affected_countries
FROM cleaned_data.life_expectancy
WHERE year = 2015 AND quality_score >= 60
GROUP BY hiv_burden_category
ORDER BY hiv_burden_category;

-- 7.6 Immunization Impact
CREATE OR REPLACE VIEW analytics.immunization_effectiveness AS
SELECT 
    CASE 
        WHEN immunization_coverage_avg >= 95 THEN '95%+ (Excellent)'
        WHEN immunization_coverage_avg >= 90 THEN '90-94% (Very Good)'
        WHEN immunization_coverage_avg >= 80 THEN '80-89% (Good)'
        WHEN immunization_coverage_avg >= 70 THEN '70-79% (Fair)'
        WHEN immunization_coverage_avg >= 60 THEN '60-69% (Poor)'
        ELSE '<60% (Very Poor)'
    END as coverage_level,
    COUNT(*) as observations,
    COUNT(DISTINCT country) as countries,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(AVG(infant_mortality_rate), 2) as avg_infant_mortality,
    ROUND(AVG(under_five_mortality_rate), 2) as avg_under5_mortality,
    ROUND(AVG(CASE WHEN population > 0 THEN measles::numeric / population * 100000 END), 1) as measles_per_100k
FROM cleaned_data.life_expectancy
WHERE year >= 2010 
    AND immunization_coverage_avg IS NOT NULL
    AND quality_score >= 60
GROUP BY coverage_level
ORDER BY 
    CASE coverage_level
        WHEN '<60% (Very Poor)' THEN 1
        WHEN '60-69% (Poor)' THEN 2
        WHEN '70-79% (Fair)' THEN 3
        WHEN '80-89% (Good)' THEN 4
        WHEN '90-94% (Very Good)' THEN 5
        ELSE 6
    END;

-- 7.7 Predictive Insights - Countries on Positive Trajectory
CREATE OR REPLACE VIEW analytics.improvement_trajectory AS
WITH trend_analysis AS (
    SELECT 
        country,
        income_group,
        region,
        -- Linear regression for trend
        REGR_SLOPE(life_expectancy, year) as annual_improvement_rate,
        REGR_R2(life_expectancy, year) as trend_reliability,
        -- Recent values
        AVG(CASE WHEN year >= 2010 THEN life_expectancy END) as recent_life_exp,
        AVG(CASE WHEN year >= 2010 THEN gdp_per_capita END) as recent_gdp,
        AVG(CASE WHEN year >= 2010 THEN health_spending_efficiency END) as recent_efficiency,
        -- Acceleration
        AVG(CASE WHEN year BETWEEN 2010 AND 2015 THEN life_expectancy END) -
        AVG(CASE WHEN year BETWEEN 2005 AND 2010 THEN life_expectancy END) as recent_acceleration
    FROM cleaned_data.life_expectancy
    WHERE quality_score >= 60
    GROUP BY country, income_group, region
    HAVING COUNT(*) >= 10 AND REGR_R2(life_expectancy, year) > 0.7
)
SELECT 
    country,
    income_group,
    region,
    ROUND(recent_life_exp, 1) as current_life_expectancy,
    ROUND(annual_improvement_rate, 3) as annual_improvement,
    ROUND(recent_life_exp + (annual_improvement_rate * 5), 1) as projected_2020,
    ROUND(recent_acceleration, 2) as acceleration_5yr,
    ROUND(trend_reliability, 3) as trend_confidence,
    CASE 
        WHEN annual_improvement_rate > 0.5 AND recent_acceleration > 0 THEN 'Rapidly Accelerating'
        WHEN annual_improvement_rate > 0.5 THEN 'Rapid Improvement'
        WHEN annual_improvement_rate > 0.25 THEN 'Steady Improvement'
        WHEN annual_improvement_rate > 0 THEN 'Slow Improvement'
        ELSE 'Stagnant/Declining'
    END as trajectory_category
FROM trend_analysis
WHERE recent_life_exp < 80  -- Focus on countries with room to improve
ORDER BY annual_improvement_rate DESC;

-- 7.8 Comprehensive Country Scorecard
CREATE OR REPLACE VIEW analytics.country_scorecard_2015 AS
WITH latest_data AS (
    SELECT *
    FROM cleaned_data.life_expectancy
    WHERE year = 2015 AND quality_score >= 60
),
rankings AS (
    SELECT 
        country,
        status,
        income_group,
        region,
        life_expectancy,
        RANK() OVER (ORDER BY life_expectancy DESC) as life_exp_rank,
        gdp_per_capita,
        RANK() OVER (ORDER BY gdp_per_capita DESC) as gdp_rank,
        total_expenditure as health_spending_pct,
        health_spending_efficiency,
        RANK() OVER (ORDER BY health_spending_efficiency DESC) as efficiency_rank,
        adult_mortality,
        hiv_aids,
        immunization_coverage_avg,
        schooling,
        health_system_performance_score,
        disease_burden_score,
        social_determinants_score
    FROM latest_data
)
SELECT 
    country,
    status as development_status,
    income_group,
    region,
    ROUND(life_expectancy, 1) as life_expectancy,
    life_exp_rank,
    ROUND(gdp_per_capita, 0) as gdp_per_capita,
    gdp_rank,
    ROUND(health_spending_pct, 2) as health_spending_pct,
    ROUND(health_spending_efficiency, 2) as efficiency_score,
    efficiency_rank,
    ROUND(immunization_coverage_avg, 1) as immunization_coverage,
    ROUND(schooling, 1) as avg_schooling_years,
    ROUND(health_system_performance_score, 1) as health_system_score,
    ROUND(disease_burden_score, 1) as disease_burden_score,
    ROUND(social_determinants_score, 1) as social_score,
    CASE 
        WHEN life_exp_rank <= 25 AND efficiency_rank <= 25 THEN 'Global Best Practice'
        WHEN efficiency_rank <= 10 THEN 'Efficiency Leader'
        WHEN life_exp_rank <= 25 THEN 'High Performer'
        WHEN life_expectancy < 60 THEN 'Critical Support Needed'
        WHEN hiv_aids > 10 THEN 'HIV Crisis'
        WHEN immunization_coverage_avg < 70 THEN 'Immunization Gap'
        ELSE 'Standard'
    END as classification
FROM rankings
ORDER BY life_exp_rank;

-- =====================================================
-- SECTION 8: FINAL DASHBOARD DATA PREPARATION
-- =====================================================
-- Purpose: Create optimized tables for visualization

-- 8.1 Create master dashboard table
DROP TABLE IF EXISTS analytics.life_expectancy_master;
CREATE TABLE analytics.life_expectancy_master AS
SELECT 
    le.*,
    -- Add improvement metrics
    le2000.life_expectancy as life_exp_2000,
    CASE 
        WHEN le.year = 2015 AND le2000.life_expectancy IS NOT NULL 
        THEN le.life_expectancy - le2000.life_expectancy 
        ELSE NULL 
    END as improvement_since_2000,
    -- Add rankings for 2015
    CASE 
        WHEN le.year = 2015 
        THEN RANK() OVER (
            PARTITION BY le.year 
            ORDER BY le.life_expectancy DESC
        ) 
        ELSE NULL 
    END as global_rank_2015
FROM cleaned_data.life_expectancy le
LEFT JOIN cleaned_data.life_expectancy le2000 
    ON le.country = le2000.country 
    AND le2000.year = 2000;

-- Create indexes for performance
CREATE INDEX idx_dashboard_country_year ON analytics.life_expectancy_master(country, year);
CREATE INDEX idx_dashboard_year ON analytics.life_expectancy_master(year);
CREATE INDEX idx_dashboard_quality ON analytics.life_expectancy_master(quality_score);

-- 8.2 Create summary statistics for dashboard
DROP TABLE IF EXISTS analytics.dashboard_summary;
CREATE TABLE analytics.dashboard_summary AS
SELECT 
    -- Global averages
    (SELECT ROUND(AVG(life_expectancy), 1) 
     FROM analytics.life_expectancy_master 
     WHERE year = 2015 AND quality_score >= 60) as global_avg_2015,
    
    (SELECT ROUND(AVG(life_expectancy), 1) 
     FROM analytics.life_expectancy_master 
     WHERE year = 2000 AND quality_score >= 60) as global_avg_2000,
    
    -- Development gap
    (SELECT ROUND(
        AVG(CASE WHEN status = 'Developed' THEN life_expectancy END) -
        AVG(CASE WHEN status = 'Developing' THEN life_expectancy END), 1)
     FROM analytics.life_expectancy_master 
     WHERE year = 2015 AND quality_score >= 60) as development_gap_2015,
    
    -- Top performer
    (SELECT country || ': ' || ROUND(life_expectancy, 1) || ' years'
     FROM analytics.life_expectancy_master
     WHERE year = 2015 AND quality_score >= 80
     ORDER BY life_expectancy DESC
     LIMIT 1) as top_country_2015,
    
    -- Most improved
    (SELECT country || ': +' || ROUND(improvement_since_2000, 1) || ' years'
     FROM analytics.life_expectancy_master
     WHERE year = 2015 AND improvement_since_2000 IS NOT NULL
     ORDER BY improvement_since_2000 DESC
     LIMIT 1) as most_improved_country,
    
    -- Efficiency champion
    (SELECT country || ': ' || ROUND(life_expectancy, 1) || ' years with ' || 
            ROUND(total_expenditure, 1) || '% GDP'
     FROM analytics.life_expectancy_master
     WHERE year = 2015 AND health_spending_efficiency IS NOT NULL
     ORDER BY health_spending_efficiency DESC
     LIMIT 1) as efficiency_champion,
    
    -- Data quality
    (SELECT COUNT(DISTINCT country)
     FROM analytics.life_expectancy_master
     WHERE year = 2015 AND quality_score >= 60) as countries_analyzed,
    
    CURRENT_TIMESTAMP as analysis_date;

-- 8.3 Export views for visualization tools
-- These can be connected to Tableau, Power BI, or custom dashboards

CREATE OR REPLACE VIEW analytics.viz_global_trends AS
SELECT * FROM analytics.global_trends;

CREATE OR REPLACE VIEW analytics.viz_country_rankings AS
SELECT * FROM analytics.country_scorecard_2015;

CREATE OR REPLACE VIEW analytics.viz_efficiency_analysis AS
SELECT * FROM analytics.healthcare_efficiency;

CREATE OR REPLACE VIEW analytics.viz_disease_impact AS
SELECT * FROM analytics.disease_burden_analysis;

CREATE OR REPLACE VIEW analytics.viz_improvements AS
SELECT * FROM analytics.country_improvements;

-- =====================================================
-- SECTION 9: DATA EXPORT QUERIES FOR DASHBOARD
-- =====================================================
-- Purpose: Final queries to extract data for visualization

-- 9.1 Summary Metrics for Dashboard Header
SELECT 
    global_avg_2015,
    global_avg_2000,
    ROUND(global_avg_2015 - global_avg_2000, 1) as global_improvement,
    development_gap_2015,
    countries_analyzed,
    most_improved_country,
    efficiency_champion
FROM analytics.dashboard_summary;

-- 9.2 Time Series Data for Line Charts
SELECT 
    year,
    status,
    ROUND(AVG(life_expectancy), 2) as avg_life_expectancy,
    COUNT(DISTINCT country) as countries
FROM analytics.life_expectancy_master
WHERE quality_score >= 60
GROUP BY year, status
ORDER BY year, status;

-- 9.3 Top 20 Countries by Life Expectancy (2015)
SELECT 
    global_rank_2015 as rank,
    country,
    ROUND(life_expectancy, 1) as life_expectancy,
    income_group,
    ROUND(gdp_per_capita, 0) as gdp_per_capita,
    ROUND(total_expenditure, 2) as health_spending_pct,
    ROUND(health_spending_efficiency, 2) as efficiency_score
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND global_rank_2015 <= 20
    AND quality_score >= 60
ORDER BY global_rank_2015;

-- 9.4 Most Improved Countries (continued)
SELECT 
    country,
    income_group,
    region,
    ROUND(life_exp_2000, 1) as life_exp_2000,
    ROUND(life_expectancy, 1) as life_exp_2015,
    ROUND(improvement_since_2000, 1) as improvement_years,
    ROUND((improvement_since_2000 / life_exp_2000) * 100, 2) as improvement_pct,
    ROUND(gdp_per_capita, 0) as gdp_per_capita_2015
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND improvement_since_2000 IS NOT NULL
    AND quality_score >= 60
ORDER BY improvement_since_2000 DESC
LIMIT 20;

-- 9.5 Healthcare Efficiency Leaders
SELECT 
    country,
    income_group,
    ROUND(life_expectancy, 1) as life_expectancy,
    ROUND(total_expenditure, 2) as health_spending_pct,
    ROUND(health_spending_efficiency, 2) as efficiency_score,
    ROUND(gdp_per_capita, 0) as gdp_per_capita,
    ROUND(immunization_coverage_avg, 1) as immunization_coverage
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND health_spending_efficiency IS NOT NULL
    AND quality_score >= 60
ORDER BY health_spending_efficiency DESC
LIMIT 15;

-- 9.6 Regional Performance Comparison
SELECT 
    region,
    COUNT(DISTINCT country) as countries,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(MIN(life_expectancy), 1) as min_life_expectancy,
    ROUND(MAX(life_expectancy), 1) as max_life_expectancy,
    ROUND(STDDEV(life_expectancy), 1) as std_deviation,
    ROUND(AVG(gdp_per_capita), 0) as avg_gdp_per_capita,
    ROUND(AVG(total_expenditure), 2) as avg_health_spending_pct,
    ROUND(AVG(immunization_coverage_avg), 1) as avg_immunization
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND region IS NOT NULL
    AND quality_score >= 60
GROUP BY region
ORDER BY avg_life_expectancy DESC;

-- 9.7 Income Group Analysis
SELECT 
    income_group,
    COUNT(DISTINCT country) as countries,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(AVG(gdp_per_capita), 0) as avg_gdp_per_capita,
    ROUND(AVG(improvement_since_2000), 1) as avg_improvement,
    ROUND(AVG(health_spending_efficiency), 2) as avg_efficiency,
    ROUND(AVG(adult_mortality), 0) as avg_adult_mortality
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND income_group IS NOT NULL
    AND quality_score >= 60
GROUP BY income_group
ORDER BY avg_life_expectancy DESC;

-- 9.8 Disease Burden Analysis by Country (HIV Focus)
SELECT 
    country,
    region,
    income_group,
    ROUND(life_expectancy, 1) as life_expectancy,
    ROUND(hiv_aids, 2) as hiv_deaths_per_1000,
    ROUND(adult_mortality, 0) as adult_mortality,
    ROUND(infant_mortality_rate, 2) as infant_mortality_rate,
    CASE 
        WHEN hiv_aids > 20 THEN 'Severe Crisis'
        WHEN hiv_aids > 10 THEN 'Major Crisis'
        WHEN hiv_aids > 5 THEN 'High Burden'
        WHEN hiv_aids > 1 THEN 'Moderate Burden'
        ELSE 'Low Burden'
    END as hiv_burden_level
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND hiv_aids IS NOT NULL
    AND quality_score >= 60
ORDER BY hiv_aids DESC;

-- 9.9 Immunization Coverage vs Outcomes
SELECT 
    CASE 
        WHEN immunization_coverage_avg >= 95 THEN '95%+ (Excellent)'
        WHEN immunization_coverage_avg >= 90 THEN '90-94% (Very Good)'
        WHEN immunization_coverage_avg >= 80 THEN '80-89% (Good)'
        WHEN immunization_coverage_avg >= 70 THEN '70-79% (Fair)'
        ELSE '<70% (Poor)'
    END as coverage_category,
    COUNT(DISTINCT country) as countries,
    ROUND(AVG(life_expectancy), 1) as avg_life_expectancy,
    ROUND(AVG(infant_mortality_rate), 2) as avg_infant_mortality,
    ROUND(AVG(under_five_mortality_rate), 2) as avg_under5_mortality,
    STRING_AGG(
        CASE WHEN immunization_coverage_avg < 70 THEN country END, 
        ', ' ORDER BY immunization_coverage_avg
    ) as low_coverage_countries
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND immunization_coverage_avg IS NOT NULL
    AND quality_score >= 60
GROUP BY coverage_category
ORDER BY 
    CASE coverage_category
        WHEN '<70% (Poor)' THEN 1
        WHEN '70-79% (Fair)' THEN 2
        WHEN '80-89% (Good)' THEN 3
        WHEN '90-94% (Very Good)' THEN 4
        ELSE 5
    END;

-- =====================================================
-- SECTION 10: DATA QUALITY REPORT
-- =====================================================
-- Purpose: Final data quality assessment for stakeholders

-- 10.1 Comprehensive Data Quality Report
WITH quality_summary AS (
    SELECT 
        'Total Records' as metric,
        COUNT(*)::text as value,
        'All years, all countries' as description
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'High Quality Records',
        COUNT(*)::text,
        'Quality score >= 80'
    FROM analytics.life_expectancy_master
    WHERE quality_score >= 80
    
    UNION ALL
    
    SELECT 
        'Countries with Complete Timeline',
        COUNT(DISTINCT country)::text,
        'Countries with data for all 16 years'
    FROM analytics.life_expectancy_master
    WHERE country IN (
        SELECT country 
        FROM analytics.life_expectancy_master 
        GROUP BY country 
        HAVING COUNT(DISTINCT year) = 16
    )
    
    UNION ALL
    
    SELECT 
        'Average Completeness Score',
        ROUND(AVG(completeness_score), 1)::text || '%',
        'Percentage of non-null fields per record'
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'Records with Life Expectancy',
        COUNT(*)::text,
        'Primary outcome variable available'
    FROM analytics.life_expectancy_master
    WHERE life_expectancy IS NOT NULL
)
SELECT * FROM quality_summary;

-- 10.2 Missing Data Analysis by Critical Variables
SELECT 
    variable_name,
    total_possible_records,
    available_records,
    missing_records,
    ROUND(100.0 * available_records / total_possible_records, 1) as availability_pct,
    CASE 
        WHEN 100.0 * available_records / total_possible_records >= 95 THEN 'Excellent'
        WHEN 100.0 * available_records / total_possible_records >= 85 THEN 'Good'
        WHEN 100.0 * available_records / total_possible_records >= 70 THEN 'Fair'
        ELSE 'Poor'
    END as data_quality
FROM (
    SELECT 
        'Life Expectancy' as variable_name,
        COUNT(*) as total_possible_records,
        COUNT(life_expectancy) as available_records,
        COUNT(*) - COUNT(life_expectancy) as missing_records
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'GDP per Capita',
        COUNT(*),
        COUNT(gdp_per_capita),
        COUNT(*) - COUNT(gdp_per_capita)
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'Health Expenditure',
        COUNT(*),
        COUNT(total_expenditure),
        COUNT(*) - COUNT(total_expenditure)
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'Immunization Coverage',
        COUNT(*),
        COUNT(immunization_coverage_avg),
        COUNT(*) - COUNT(immunization_coverage_avg)
    FROM analytics.life_expectancy_master
    
    UNION ALL
    
    SELECT 
        'Years of Schooling',
        COUNT(*),
        COUNT(schooling),
        COUNT(*) - COUNT(schooling)
    FROM analytics.life_expectancy_master
) t
ORDER BY availability_pct DESC;

-- =====================================================
-- SECTION 11: FINAL INSIGHTS AND RECOMMENDATIONS
-- =====================================================
-- Purpose: Executive summary queries for decision makers

-- 11.1 Key Findings Summary
WITH key_insights AS (
    -- Global improvement
    SELECT 
        'Global Progress' as insight_category,
        'Life expectancy increased by ' || 
        ROUND(
            (SELECT AVG(life_expectancy) FROM analytics.life_expectancy_master WHERE year = 2015 AND quality_score >= 60) -
            (SELECT AVG(life_expectancy) FROM analytics.life_expectancy_master WHERE year = 2000 AND quality_score >= 60), 1
        ) || ' years globally from 2000-2015' as finding,
        1 as priority_order
    
    UNION ALL
    
    -- Development gap
    SELECT 
        'Development Gap',
        'Gap between developed and developing countries: ' ||
        ROUND(
            AVG(CASE WHEN status = 'Developed' THEN life_expectancy END) -
            AVG(CASE WHEN status = 'Developing' THEN life_expectancy END), 1
        ) || ' years in 2015',
        2
    FROM analytics.life_expectancy_master 
    WHERE year = 2015 AND quality_score >= 60
    
    UNION ALL
    
    -- HIV impact
    SELECT 
        'HIV/AIDS Impact',
        COUNT(DISTINCT country) || ' countries have severe HIV burden (>10 deaths per 1000 births)',
        3
    FROM analytics.life_expectancy_master
    WHERE year = 2015 AND hiv_aids > 10
    
    UNION ALL
    
    -- Immunization gaps
    SELECT 
        'Immunization Gaps',
        COUNT(DISTINCT country) || ' countries have immunization coverage below 80%',
        4
    FROM analytics.life_expectancy_master
    WHERE year = 2015 AND immunization_coverage_avg < 80
    
    UNION ALL
    
    -- Economic correlation
    SELECT 
        'Economic Factors',
        'Strong correlation between education, income composition and life expectancy',
        5
)
SELECT * FROM key_insights ORDER BY priority_order;

-- 11.2 Countries Requiring Immediate Attention
SELECT 
    'Critical Priority' as urgency_level,
    country,
    region,
    ROUND(life_expectancy, 1) as life_expectancy,
    ROUND(hiv_aids, 1) as hiv_burden,
    ROUND(immunization_coverage_avg, 1) as immunization_coverage,
    'Life expectancy <55 OR HIV >15' as criteria
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND quality_score >= 60
    AND (life_expectancy < 55 OR hiv_aids > 15)

UNION ALL

SELECT 
    'High Priority',
    country,
    region,
    ROUND(life_expectancy, 1),
    ROUND(hiv_aids, 1),
    ROUND(immunization_coverage_avg, 1),
    'Life expectancy 55-65 OR immunization <70%'
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND quality_score >= 60
    AND life_expectancy BETWEEN 55 AND 65
    AND (hiv_aids > 5 OR immunization_coverage_avg < 70)
    AND NOT (life_expectancy < 55 OR hiv_aids > 15)

ORDER BY 
    CASE urgency_level WHEN 'Critical Priority' THEN 1 ELSE 2 END,
    life_expectancy;

-- 11.3 Best Practice Examples
SELECT 
    'Efficiency Leaders' as category,
    country,
    income_group,
    ROUND(life_expectancy, 1) as life_expectancy,
    ROUND(total_expenditure, 2) as health_spending_pct,
    ROUND(health_spending_efficiency, 2) as efficiency_score,
    'High life expectancy with moderate spending' as reason
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND health_spending_efficiency > 12
    AND quality_score >= 80

UNION ALL

SELECT 
    'Rapid Improvers',
    country,
    income_group,
    ROUND(life_expectancy, 1),
    ROUND(total_expenditure, 2),
    ROUND(improvement_since_2000, 1),
    'Gained >10 years since 2000'
FROM analytics.life_expectancy_master
WHERE year = 2015 
    AND improvement_since_2000 > 10
    AND quality_score >= 60

ORDER BY category, life_expectancy DESC;

-- =====================================================
-- SECTION 12: DATA EXPORT FOR EXTERNAL TOOLS
-- =====================================================
-- Purpose: Prepare data for export to CSV/Excel for further analysis

-- 12.1 Master dataset for export
CREATE OR REPLACE VIEW analytics.export_master_dataset AS
SELECT 
    country,
    year,
    status as development_status,
    income_group,
    region,
    life_expectancy,
    adult_mortality,
    infant_deaths,
    alcohol_consumption as alcohol,
    percentage_expenditure,
    hepatitis_b_coverage as hepatitis_b,
    measles_cases as measles,
    bmi,
    under_five_deaths,
    polio_coverage as polio,
    total_expenditure as health_spending_pct_gdp,
    diphtheria_coverage as diphtheria,
    hiv_aids_deaths_per_1000 as hiv_aids,
    gdp,
    population,
    thinness_1_19_years,
    thinness_5_9_years,
    income_composition_of_resources,
    schooling,
    gdp_per_capita,
    health_expenditure_per_capita,
    infant_mortality_rate,
    under_five_mortality_rate,
    immunization_coverage_avg,
    completeness_score,
    quality_score,
    improvement_since_2000,
    global_rank_2015
FROM analytics.life_expectancy_master
WHERE quality_score >= 60;

-- 12.2 Summary statistics for export
CREATE OR REPLACE VIEW analytics.export_country_summary AS
SELECT 
    country,
    MAX(CASE WHEN year = 2015 THEN income_group END) as income_group,
    MAX(CASE WHEN year = 2015 THEN region END) as region,
    -- 2000 values
    MAX(CASE WHEN year = 2000 THEN life_expectancy END) as life_exp_2000,
    MAX(CASE WHEN year = 2000 THEN gdp_per_capita END) as gdp_per_capita_2000,
    -- 2015 values
    MAX(CASE WHEN year = 2015 THEN life_expectancy END) as life_exp_2015,
    MAX(CASE WHEN year = 2015 THEN gdp_per_capita END) as gdp_per_capita_2015,
    MAX(CASE WHEN year = 2015 THEN global_rank_2015 END) as global_rank_2015,
    -- Improvements
    MAX(CASE WHEN year = 2015 THEN improvement_since_2000 END) as improvement_years,
    -- 2015 health metrics
    MAX(CASE WHEN year = 2015 THEN total_expenditure END) as health_spending_pct_2015,
    MAX(CASE WHEN year = 2015 THEN immunization_coverage_avg END) as immunization_2015,
    MAX(CASE WHEN year = 2015 THEN hiv_aids END) as hiv_burden_2015,
    MAX(CASE WHEN year = 2015 THEN schooling END) as schooling_2015,
    -- Data quality
    AVG(quality_score) as avg_quality_score,
    COUNT(*) as years_of_data
FROM analytics.life_expectancy_master
WHERE quality_score >= 60
GROUP BY country
HAVING COUNT(*) >= 10;

-- =====================================================
-- SECTION 13: CLEANUP AND MAINTENANCE
-- =====================================================
-- Purpose: Database maintenance and optimization

-- 13.1 Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_life_exp_country_year ON analytics.life_expectancy_master(country, year);
CREATE INDEX IF NOT EXISTS idx_life_exp_region ON analytics.life_expectancy_master(region);
CREATE INDEX IF NOT EXISTS idx_life_exp_income ON analytics.life_expectancy_master(income_group);
CREATE INDEX IF NOT EXISTS idx_life_exp_quality ON analytics.life_expectancy_master(quality_score);

-- 13.2 Update table statistics
ANALYZE analytics.life_expectancy_master;
ANALYZE cleaned_data.life_expectancy;

-- 13.3 Create backup of cleaned data
CREATE TABLE IF NOT EXISTS analytics.life_expectancy_backup AS
SELECT * FROM analytics.life_expectancy_master;

-- =====================================================
-- PROJECT COMPLETION SUMMARY
-- =====================================================
-- Purpose: Final verification and project summary

-- Final verification query
SELECT 
    'PROJECT COMPLETION SUMMARY' as status,
    'Total Countries Analyzed: ' || COUNT(DISTINCT country) as metric1,
    'Years of Data: ' || COUNT(DISTINCT year) as metric2,
    'High Quality Records: ' || SUM(CASE WHEN quality_score >= 80 THEN 1 ELSE 0 END) as metric3,
    'Analysis Date: ' || CURRENT_DATE as metric4
FROM analytics.life_expectancy_master;

-- =====================================================
-- END OF LIFE EXPECTANCY ANALYSIS PROJECT
-- =====================================================
-- 
-- This comprehensive analysis provides:
-- 1. Clean, validated dataset with quality scores
-- 2. Multiple analytical perspectives on global health
-- 3. Identification of best practices and areas needing attention
-- 4. Ready-to-use data for visualization platforms
-- 5. Actionable insights for policy makers
--
-- Next Steps:
-- 1. Connect to visualization tools (Tableau, Power BI, etc.)
-- 2. Implement real-time data updates from WHO/World Bank APIs
-- 3. Extend analysis to include more recent years
-- 4. Add predictive modeling capabilities
-- 5. Create automated reporting workflows
--
-- For questions or modifications, contact: Lloyd Dela Cruz
-- =====================================================