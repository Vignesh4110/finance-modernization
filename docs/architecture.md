# System Architecture

## Current Architecture (Local Development)
```
┌─────────────────────────────────────────────────────────────────┐
│                        LOCAL DEVELOPMENT                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   AS400      │    │   Python     │    │   DuckDB     │      │
│  │   Files      │───▶│   Parser     │───▶│   Database   │      │
│  │  (Mock)      │    │              │    │              │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                                                 │                │
│                                                 ▼                │
│                      ┌──────────────────────────────────┐       │
│                      │           dbt Core               │       │
│                      │   Bronze → Silver → Gold         │       │
│                      └──────────────────────────────────┘       │
│                                                 │                │
│         ┌───────────────────┬─────────────────┼────────┐       │
│         ▼                   ▼                 ▼        ▼       │
│  ┌────────────┐    ┌────────────┐    ┌──────────┐ ┌────────┐  │
│  │  Airflow   │    │  ML Models │    │   LLM    │ │Streamlit│  │
│  │   DAGs     │    │  (sklearn) │    │  (Groq)  │ │Dashboard│  │
│  └────────────┘    └────────────┘    └──────────┘ └────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Production Architecture (AWS)
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS CLOUD                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐                                                        │
│  │   AS400/IBM i   │                                                        │
│  │   (On-Premise)  │                                                        │
│  └────────┬────────┘                                                        │
│           │ SFTP / CDC                                                       │
│           ▼                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │   AWS Transfer  │    │    AWS Lambda   │    │    Amazon S3    │         │
│  │     Family      │───▶│   (Parser)      │───▶│  (Data Lake)    │         │
│  │                 │    │                 │    │  Bronze Layer   │         │
│  └─────────────────┘    └─────────────────┘    └────────┬────────┘         │
│                                                          │                   │
│                                                          ▼                   │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                        Amazon MWAA                                │      │
│  │                   (Managed Airflow)                               │      │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐ │      │
│  │  │ Extraction │  │ Transform  │  │  ML Train  │  │  LLM Jobs  │ │      │
│  │  │    DAG     │  │    DAG     │  │    DAG     │  │    DAG     │ │      │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘ │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │   Amazon S3     │    │    Snowflake    │    │   Amazon S3     │         │
│  │  Silver Layer   │───▶│   (Warehouse)   │◀───│   Gold Layer    │         │
│  │                 │    │                 │    │                 │         │
│  └─────────────────┘    └────────┬────────┘    └─────────────────┘         │
│                                  │                                          │
│         ┌────────────────────────┼────────────────────────┐                │
│         ▼                        ▼                        ▼                │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │   Amazon        │    │    AWS          │    │   Streamlit     │         │
│  │   SageMaker     │    │    Bedrock      │    │   on ECS        │         │
│  │   (ML Models)   │    │   (LLM API)     │    │   (Dashboard)   │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
│                                                          │                   │
│                                                          ▼                   │
│                                                 ┌─────────────────┐         │
│                                                 │   CloudFront    │         │
│                                                 │   (CDN)         │         │
│                                                 └─────────────────┘         │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────┐      │
│  │                      Monitoring & Security                        │      │
│  │  CloudWatch │ X-Ray │ IAM │ Secrets Manager │ VPC │ WAF          │      │
│  └──────────────────────────────────────────────────────────────────┘      │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Component Mapping

| Local Component | AWS Production Equivalent |
|-----------------|---------------------------|
| Mock AS400 Files | AWS Transfer Family + SFTP from real AS400 |
| Python Parser | AWS Lambda Functions |
| DuckDB | Amazon S3 (Bronze) + Snowflake |
| dbt Core | dbt Cloud or dbt on MWAA |
| Airflow (Docker) | Amazon MWAA |
| scikit-learn/XGBoost | Amazon SageMaker |
| Groq API | AWS Bedrock (Claude/Llama) |
| Streamlit (local) | ECS Fargate + CloudFront |
| .env secrets | AWS Secrets Manager |

## Data Flow

1. **Extract**: AS400 files transferred via SFTP to S3 Bronze layer
2. **Transform**: Airflow triggers dbt to process Bronze → Silver → Gold
3. **Load**: Gold layer loaded to Snowflake for analytics
4. **ML**: SageMaker trains models on schedule, stores in S3
5. **Serve**: Streamlit on ECS serves dashboard, calls Bedrock for LLM
6. **Monitor**: CloudWatch dashboards, alerts, and X-Ray tracing

## Cost Estimate (Monthly)

| Service | Estimated Cost |
|---------|----------------|
| S3 (100GB) | $2.30 |
| MWAA (smallest) | $50 |
| Lambda | $5 |
| Snowflake (XS) | $40 |
| SageMaker (inference) | $30 |
| ECS Fargate | $20 |
| CloudFront | $5 |
| **Total** | **~$150/month** |

*Note: Costs can be reduced with reserved capacity and optimization*
