# Global Life Expectancy Analysis (2000-2015)

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![SQL](https://img.shields.io/badge/SQL-005C84?style=for-the-badge&logo=mysql&logoColor=white)](https://en.wikipedia.org/wiki/SQL)
[![Data Analysis](https://img.shields.io/badge/Data%20Analysis-FF6B6B?style=for-the-badge&logo=chartdotjs&logoColor=white)](#)

## 📊 Project Overview

A comprehensive data analysis project examining global life expectancy trends across 193 countries from 2000-2015. This project provides actionable insights into health outcomes, policy effectiveness, and socioeconomic factors that influence population health worldwide.

### 🎯 Key Objectives

- **Analyze Global Health Trends**: Identify patterns in life expectancy improvements across developed and developing nations
- **Healthcare Efficiency Assessment**: Evaluate which countries achieve optimal health outcomes with available resources
- **Policy Impact Analysis**: Examine the effectiveness of immunization programs, healthcare spending, and social determinants
- **Data-Driven Recommendations**: Provide evidence-based insights for health policy decision-making

## 🚀 Key Findings

### Global Progress
- **Global life expectancy increased by 4.2 years** from 2000-2015
- **Development gap**: 12.8 years between developed and developing countries (2015)
- **22 countries** gained more than 10 years in life expectancy during this period

### Healthcare Efficiency Champions
| Country | Life Expectancy | Health Spending (% GDP) | Efficiency Score |
|---------|----------------|------------------------|------------------|
| South Korea | 82.3 years | 7.2% | 11.4 |
| Japan | 83.7 years | 10.9% | 7.7 |
| Singapore | 82.1 years | 4.9% | 16.8 |

### Critical Findings
- **15 countries** face severe HIV burden (>10 deaths per 1000 births)
- **34 countries** have immunization coverage below 80%
- **Strong correlation** between education, income composition, and life expectancy (r > 0.8)

## 🗂️ Project Structure

```
Life-Expectancy-Analysis/
├── README.md                   # Project documentation
├── Life Expectancy Data.csv    # Raw dataset (WHO, 2000-2015)
└── SQL/
    └── LifeExpectancy.sql      # Complete PostgreSQL analysis workflow
```

## 🛠️ Technical Implementation

### Database Architecture
- **Raw Data Schema**: Original data with validation checks
- **Cleaned Data Schema**: Standardized, quality-controlled dataset
- **External Data Schema**: World Bank, WHO integration capabilities
- **Analytics Schema**: Analysis views and summary tables

### Analysis Pipeline
1. **Data Quality Assessment** - Missing value analysis, outlier detection
2. **Data Cleaning & Standardization** - Biological constraints, country name standardization
3. **Feature Engineering** - Calculated metrics, composite health indicators
4. **Comprehensive Analysis** - Trends, correlations, efficiency metrics
5. **Dashboard Preparation** - Optimized views for visualization

## 📈 Analysis Sections

### 1. Global Trends Analysis
- Year-over-year improvements by development status
- Development gap evolution
- Regional performance comparisons

### 2. Country Performance Metrics
- **Success Stories**: Countries with exceptional improvements
- **Efficiency Leaders**: High life expectancy with moderate spending
- **Priority Cases**: Countries requiring immediate attention

### 3. Health System Analysis
- Healthcare spending efficiency
- Immunization program effectiveness
- Disease burden impact (HIV/AIDS focus)

### 4. Predictive Insights
- Countries on positive trajectory
- Trend reliability analysis
- 2020 projections based on historical data

## 🎯 Key Performance Indicators

### Health Outcomes
- Life expectancy rankings and improvements
- Adult mortality rates
- Infant and under-5 mortality rates

### Health System Performance
- Healthcare spending efficiency (Life Expectancy / % GDP spent)
- Immunization coverage rates
- Disease burden scores

### Social Determinants
- Education levels (years of schooling)
- Income composition index
- Nutritional status indicators

## 🔧 Setup & Installation

### Prerequisites
- PostgreSQL 12+ 
- Database administration tool (pgAdmin, DBeaver, or VS Code PostgreSQL extension)

### Quick Start
1. **Clone the repository**
   ```bash
   git clone https://github.com/lloyd-delacruz/LifeExpectancy.git
   cd LifeExpectancy
   ```

2. **Create PostgreSQL database**
   ```sql
   CREATE DATABASE life_expectancy_analysis;
   \c life_expectancy_analysis;
   ```

3. **Run the analysis**
   ```bash
   psql -d life_expectancy_analysis -f SQL/LifeExpectancy.sql
   ```

4. **Load the dataset**
   - Update the file path in the SQL script (line 74)
   - Execute the COPY command or use your database tool's import wizard

## 📊 Data Specifications

### Dataset Details
- **Source**: World Health Organization (WHO)
- **Coverage**: 193 countries, 2000-2015 (16 years)
- **Total Records**: 2,938 observations
- **Quality Score**: 85% completeness average

### Variables (22 indicators)
- **Demographics**: Country, year, development status, population
- **Health Outcomes**: Life expectancy, adult mortality, infant deaths
- **Health Behaviors**: Alcohol consumption, BMI, thinness prevalence
- **Healthcare System**: Immunization coverage, health expenditure
- **Socioeconomic**: GDP, education, income composition
- **Disease Specific**: HIV/AIDS, measles cases

## 🎨 Visualization Ready

The analysis generates multiple views optimized for visualization platforms:
- `viz_global_trends` - Time series data for trend analysis
- `viz_country_rankings` - Country performance scorecards
- `viz_efficiency_analysis` - Healthcare efficiency metrics
- `viz_disease_impact` - Disease burden analysis
- `viz_improvements` - Country improvement trajectories

## 📋 Quality Assurance

### Data Validation
- **Biological Constraints**: Life expectancy caps, mortality rate limits
- **Logical Validation**: Population sanity checks, percentage boundaries
- **External Validation**: Cross-reference with World Bank data
- **Quality Scoring**: Completeness metrics for each record

### Analysis Standards
- **Reproducible Workflow**: Fully documented SQL procedures
- **Performance Optimized**: Indexed tables for fast queries
- **Error Handling**: Comprehensive data cleaning procedures
- **Documentation**: Inline comments explaining each analytical step

## 🔍 Usage Examples

### Find Top Performing Countries (2015)
```sql
SELECT country, life_expectancy, health_spending_efficiency
FROM analytics.country_scorecard_2015
WHERE life_exp_rank <= 10
ORDER BY life_exp_rank;
```

### Analyze Healthcare Efficiency
```sql
SELECT * FROM analytics.healthcare_efficiency
WHERE efficiency_category = 'Very Efficient'
ORDER BY avg_efficiency_score DESC;
```

### Track Country Improvements
```sql
SELECT country, improvement_years, progress_category
FROM analytics.country_improvements
WHERE improvement_years > 10
ORDER BY improvement_years DESC;
```

## 🎓 Academic Applications

This project serves as a comprehensive case study for:
- **Public Health Policy Analysis**
- **Healthcare Economics Research**
- **Data Science Portfolio Development**
- **SQL Advanced Analytics Training**
- **Global Health Disparities Studies**

## 🔮 Future Enhancements

- [ ] **Real-time Updates**: Integration with WHO/World Bank APIs
- [ ] **Predictive Modeling**: Machine learning for life expectancy forecasting
- [ ] **Interactive Dashboard**: Web-based visualization platform
- [ ] **Extended Timeline**: Include 2016-2023 data when available
- [ ] **Geospatial Analysis**: Mapping capabilities for regional insights

## 📚 References & Data Sources

- **World Health Organization (WHO)** - Global Health Observatory data
- **World Bank** - Country classifications and economic indicators
- **Our World in Data** - Methodology validation
- **UN Statistics Division** - Country standardization references

## 👨‍💻 Author

**Lloyd Dela Cruz**  
*Data Science Student | Eastern University*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/lloyd-delacruz)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/lloyd-delacruz)

## 🙏 Acknowledgments

- **Kaggle.com** for providing comprehensive global health data
- **BrianStation Vancouver** Data Analytics course for academic guidance
- **Open source community** for PostgreSQL and analytical tools
- **AI Assistants** (Claude AI, ChatGPT, Perplexity AI) for development support

---

*"Life expectancy is the most fundamental measure of population health. This analysis identifies successful health policies, efficient healthcare systems, and critical factors that can guide interventions to save lives."*

**⭐ If you find this analysis valuable, please consider starring this repository!**
