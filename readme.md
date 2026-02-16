# 💰 Finance Modernization Platform

[![CI Pipeline](https://github.com/Vignesh4110/finance-modernization/actions/workflows/ci.yml/badge.svg)](https://github.com/Vignesh4110/finance-modernization/actions/workflows/ci.yml)
[![Python 3.10](https://img.shields.io/badge/python-3.10-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange.svg)](https://www.getdbt.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.31-red.svg)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **Transforming Legacy AS400 Financial Systems into Modern Cloud-Native Architecture**

A complete end-to-end data engineering project demonstrating the modernization of a legacy IBM AS400/DB2 Accounts Receivable system into a modern data platform with ML and LLM capabilities.

## 🎯 Project Overview

This project simulates modernizing a legacy AS400-based finance system that handles:
- **Accounts Receivable (AR)** processing
- **Invoice Management** 
- **Payment Processing**
- **Collections Workflow**
- **Financial Reporting**

### The Problem (Legacy System)
- RPGLE batch programs running on IBM i
- Fixed-width files with CYYMMDD dates and packed decimals
- Nightly batch jobs via WRKJOBSCDE
- Green screen interfaces (5250)
- No real-time visibility, manual collections process

### The Solution (Modern Platform)
- Python-based data extraction with custom AS400 parser
- dbt for data transformation (Bronze → Silver → Gold)
- Apache Airflow for orchestration
- ML models for payment prediction and risk scoring
- LLM agents (Groq/Llama) for intelligent automation
- Interactive Streamlit dashboard

## 🏗️ Architecture
```
AS400 Legacy          Modern Platform
┌──────────────┐     ┌─────────────────────────────────────┐
│ RPGLE/CL     │     │  Python Extraction                  │
│ DB2/400      │ ──▶ │  dbt Transformations                │
│ WRKJOBSCDE   │     │  Airflow Orchestration              │
│ Query/400    │     │  ML Models + LLM Agents             │
│ Green Screen │     │  Streamlit Dashboard                │
└──────────────┘     └─────────────────────────────────────┘
```

[View Full Architecture Diagram](docs/architecture.md)

## 🛠️ Tech Stack

| Layer | Technologies |
|-------|-------------|
| **Data Extraction** | Python, Custom AS400 Parser (CYYMMDD dates, fixed-width files) |
| **Storage** | DuckDB (dev), Snowflake/BigQuery (prod) |
| **Transformation** | dbt Core (Bronze → Silver → Gold medallion architecture) |
| **Orchestration** | Apache Airflow |
| **Machine Learning** | scikit-learn, XGBoost, MLflow |
| **LLM Integration** | Groq (Llama 3.3 70B) - FREE! |
| **Dashboard** | Streamlit, Plotly |
| **CI/CD** | GitHub Actions |
| **Infrastructure** | Docker, Terraform (AWS architecture documented) |

## 📁 Project Structure
```
finance-modernization/
├── src/
│   ├── ingestion/           # AS400 file parsers
│   │   ├── as400_parser.py  # Fixed-width file parser
│   │   ├── file_layouts.py  # Copybook definitions
│   │   └── generate_as400_files.py
│   ├── ml/                  # Machine learning models
│   │   ├── models/
│   │   │   ├── payment_propensity.py
│   │   │   └── collection_scorer.py
│   │   └── features/
│   └── llm_agents/          # LLM-powered agents
│       └── agents/
│           ├── ar_query_agent.py      # Natural language queries
│           ├── collections_agent.py   # Email generation
│           └── legacy_documenter.py   # Code documentation
├── dbt_project/             # dbt transformations
│   ├── models/
│   │   ├── staging/         # Bronze → Silver
│   │   └── marts/           # Silver → Gold
│   └── seeds/               # Source data
├── airflow/                 # Airflow DAGs
│   └── dags/
├── streamlit_app/           # Dashboard application
│   └── app.py
├── scripts/                 # Utility scripts
│   ├── generate_seed_data.py
│   ├── run_tests.py
│   └── rebuild_all.py
├── docs/                    # Documentation
│   ├── architecture.md
│   └── legacy_system/       # AS400 documentation
├── data/                    # Data files
│   └── mock_as400/          # Simulated AS400 exports
└── tests/                   # Test files
```

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- Git
- Docker (optional, for Airflow)

### Installation
```bash
# Clone the repository
git clone https://github.com/Vignesh4110/finance-modernization.git
cd finance-modernization

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Generate seed data
python scripts/generate_seed_data.py

# Run dbt models
cd dbt_project
dbt seed
dbt run
cd ..

# Launch dashboard
streamlit run streamlit_app/app.py
```

### Set up Groq API (Free)
1. Go to [console.groq.com](https://console.groq.com)
2. Create free account and get API key
3. Create `.env` file:
```bash
echo "GROQ_API_KEY=your_key_here" > .env
```

## 📊 Features

### 1. AS400 Data Parser
- Parses fixed-width files (CPYTOIMPF exports)
- Converts CYYMMDD dates to standard format
- Handles packed decimal fields
- Generates copybook documentation

### 2. dbt Data Models
- **Staging (Silver)**: Clean and standardize raw data
- **Marts (Gold)**: Business-ready analytics tables
- **Tests**: Data quality validation
- **Documentation**: Auto-generated data catalog

### 3. ML Pipeline
- **Payment Propensity Model**: Predicts likelihood of payment
- **Collection Priority Scorer**: Ranks accounts for collection
- **Risk Categorization**: Classifies customers by risk level

### 4. LLM Agents
- **AR Query Agent**: Natural language to SQL queries
- **Collections Agent**: Generates personalized dunning emails
- **Legacy Documenter**: Auto-documents RPGLE programs

### 5. Interactive Dashboard
- Real-time AR metrics and KPIs
- Aging analysis with drill-down
- Customer risk visualization
- AI-powered query interface
- Collection worklist with email generation

## 🧪 Testing
```bash
# Run all tests
python scripts/run_tests.py

# Run dbt tests
cd dbt_project
dbt test
```

## 📈 Sample Data

The project includes realistic mock data:
- **500 customers** across 4 segments
- **5,000 invoices** with realistic aging distribution
- **1,200+ payments** with various methods
- **10,000 GL entries** for financial tracking

## 🎓 Learning Outcomes

This project demonstrates proficiency in:
- Legacy system analysis and modernization strategies
- Data engineering with Python and SQL
- dbt for analytics engineering
- Apache Airflow for workflow orchestration
- Machine learning for business applications
- LLM integration for intelligent automation
- Full-stack dashboard development
- CI/CD and DevOps practices

## 👤 Author

**Vignesh**
- Data Analytics Engineering @ Northeastern University
- Background in AS400/IBM i, DB2, RPGLE
- [GitHub](https://github.com/Vignesh4110)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Anthropic for Claude AI assistance
- Groq for free LLM API access
- dbt Labs for the amazing transformation framework
- Streamlit for the dashboard framework
