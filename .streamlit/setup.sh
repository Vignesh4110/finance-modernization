#!/bin/bash
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml << 'PROFILE'
finance_dw:
  outputs:
    dev:
      type: duckdb
      path: ./data/finance.duckdb
      threads: 4
  target: dev
PROFILE
