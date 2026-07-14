-- Schemas do projeto (único Postgres, 4 schemas)
-- Executado na inicialização do container postgres

-- OLTP: dados sintéticos gerados pelo Faker (fonte do pipeline batch)
CREATE SCHEMA IF NOT EXISTS oltp;

-- Airflow: metadados do orquestrador (tabelas criadas pelo airflow db migrate)
CREATE SCHEMA IF NOT EXISTS airflow;

-- Marquez: lineage OpenLineage (tabelas criadas pelo próprio Marquez)
CREATE SCHEMA IF NOT EXISTS marquez;

-- Gold DW: tabelas dimensionais e fatos para Metabase/Trino
CREATE SCHEMA IF NOT EXISTS gold_dw;

-- Metabase usa o schema padrão public para seus metadados
-- Precisa da extensão citext (CREATE EXTENSION requer superuser — pré-cria aqui)
CREATE EXTENSION IF NOT EXISTS citext;
