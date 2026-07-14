-- OLTP schema: tabela de pacientes sintéticos gerada via Faker.
-- Populada pelo script scripts/seed_oltp.py na inicialização do ambiente local.
-- Bronze job lê via psycopg2 (snapshot diário). CONTÉM PII — acesso restrito.

SET search_path = oltp;

CREATE TABLE IF NOT EXISTS oltp.paciente (
    id_paciente          BIGSERIAL    PRIMARY KEY,
    cpf                  CHAR(11)     NOT NULL UNIQUE,   -- PII: apenas Bronze acessa
    nome                 VARCHAR(120) NOT NULL,           -- PII
    data_nascimento      DATE         NOT NULL,           -- PII (generalizado no Silver)
    sexo                 CHAR(1)      NOT NULL CHECK (sexo IN ('M','F','O')),
    cep                  CHAR(8)      NOT NULL,           -- PII (truncado no Silver)
    municipio_codigo_ibge CHAR(7)     NOT NULL,
    email                VARCHAR(120),                    -- PII (suprimido no Silver)
    telefone             VARCHAR(20),                     -- PII (suprimido no Silver)
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_oltp_paciente_updated ON oltp.paciente (updated_at);
