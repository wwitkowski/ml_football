#!/bin/bash

psql -U airflow -d mlfootball << EOF
SET ROLE airflow;
CREATE USER mlfootball_api WITH PASSWORD '$POSTGRES_DATA_PASSWORD';
EOF