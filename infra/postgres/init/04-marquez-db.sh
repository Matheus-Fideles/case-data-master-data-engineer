#!/usr/bin/env bash
# Cria o banco de dados 'marquez' para o serviço de data lineage.
# CREATE DATABASE não pode rodar dentro de transação (DO $$), por isso é um .sh.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE marquez OWNER marquez'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquez')\gexec
EOSQL
