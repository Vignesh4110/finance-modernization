# Finance Modernization Platform

> Modernizing legacy AS400 financial systems into a cloud-native, AI-powered finance operations platform

## 🎯 Project Overview

This project demonstrates an end-to-end data engineering solution that migrates a legacy AS400-based Accounts Receivable system to a modern cloud architecture with ML-powered automation.

### Business Problem

Legacy finance systems running on AS400/IBM i suffer from:
- Manual cash application with 70% accuracy
- No intelligent collections prioritization
- 48-72 hour lag in financial visibility
- Month-end close taking 10+ days
- Zero predictive capabilities

### Solution

A modern data platform featuring:
- **Real-time data ingestion** from legacy DB2/400
- **Automated cash application** using ML matching
- **Intelligent collections** with payment propensity scoring
- **Cash flow forecasting** at daily/weekly/monthly granularity
- **LLM-powered agents** for customer outreach and audit support

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   AS400/DB2     │────▶│   Bronze Layer  │────▶│   Silver Layer  │
│   (Mock Data)   │ CDC │   (Raw Data)    │ dbt │   (Cleaned)     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                        ┌────────────────────────────────┤
                        ▼                                ▼
               ┌─────────────────┐              ┌─────────────────┐
               │   Gold Layer    │              │  ML Feature     │
               │   (Analytics)   │              │  Store          │
               └─────────────────┘              └─────────────────┘
                        │                                │
                        ▼                                ▼
               ┌─────────────────┐              ┌─────────────────┐
               │   Streamlit     │              │   ML Models     │
               │   Dashboard     │              │   + LLM Agents  │
               └─────────────────┘              └─────────────────┘
```

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Source Simulation | Python, Faker |
| Storage | PostgreSQL, AWS S3 |
| Transformation | dbt Core |
| Orchestration | Apache Airflow |
| ML Platform | scikit-learn, XGBoost, MLflow |
| LLM Agents | LangChain, Claude API |
| Visualization | Streamlit |
| Infrastructure | Docker, Terraform |

## 📁 Project Structure

```
finance-modernization/
├── src/                    # Python source code
│   ├── ingestion/          # Data extraction scripts
│   ├── transformations/    # Python transformations
│   ├── ml/                 # ML model code
│   ├── llm_agents/         # LangChain agents
│   └── utils/              # Shared utilities
├── dbt_project/            # dbt models and tests
│   ├── models/
│   │   ├── staging/        # Bronze → Silver
│   │   ├── intermediate/   # Business logic
│   │   └── marts/          # Gold layer
│   └── seeds/              # Static reference data
├── airflow/                # Orchestration
│   └── dags/               # DAG definitions
├── data/                   # Data files
│   └── mock_as400/         # Simulated legacy data
├── notebooks/              # Exploration notebooks
├── tests/                  # Unit and integration tests
├── infra/                  # Infrastructure code
│   ├── docker/             # Docker configurations
│   └── terraform/          # AWS infrastructure
├── streamlit_app/          # Dashboard application
└── docs/                   # Documentation
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Docker Desktop
- Git

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/finance-modernization.git
cd finance-modernization

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running Locally

```bash
# Start infrastructure (PostgreSQL, Airflow)
docker-compose up -d

# Run dbt models
cd dbt_project && dbt run

# Start Streamlit dashboard
streamlit run streamlit_app/app.py
```

## 📊 Key Features

### 1. Automated Cash Application
ML model that matches incoming payments to open invoices with 95%+ accuracy.

### 2. Collections Prioritization
Payment propensity scoring that ranks AR accounts by likelihood to pay.

### 3. Cash Flow Forecasting
Time-series forecasting for daily/weekly/monthly cash position.

### 4. LLM Collections Agent
Automated, personalized dunning emails with tone and compliance guardrails.

## 📈 Results

| Metric | Before | After |
|--------|--------|-------|
| Cash Application Accuracy | 70% | 95% |
| DSO (Days Sales Outstanding) | 45 days | 32 days |
| Manual Collections Effort | 100% | 20% |
| Month-End Close | 10 days | 3 days |

## 👤 Author

**Vignesh** - Data Analytics Engineering @ Northeastern University
