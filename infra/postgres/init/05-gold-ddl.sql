-- Gold DW: apenas as tabelas SCD Tipo 2 que o Spark mantém via psycopg2.
-- Todas as demais dimensões e fatos vivem em Delta Lake no MinIO (ADR-0011).

SET search_path = gold_dw;

-- SCD Type 2: dim_paciente (hash-only, sem PII — ADR-0004 + ADR-0006)
CREATE TABLE IF NOT EXISTS gold_dw.dim_paciente (
    sk_paciente       SERIAL      PRIMARY KEY,
    id_paciente_hash  VARCHAR(64) NOT NULL,
    sexo              CHAR(1),
    ano_nascimento    SMALLINT,
    cep_regiao        CHAR(3),
    municipio_codigo_ibge VARCHAR(7),
    dt_inicio         DATE        NOT NULL DEFAULT CURRENT_DATE,
    dt_fim            DATE        NOT NULL DEFAULT '9999-12-31',
    is_current        BOOLEAN     NOT NULL DEFAULT TRUE,
    _batch_id         VARCHAR(64),
    _load_ts          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paciente_hash    ON gold_dw.dim_paciente (id_paciente_hash, is_current);
CREATE INDEX IF NOT EXISTS idx_paciente_current ON gold_dw.dim_paciente (is_current) WHERE is_current = TRUE;

-- SCD Type 2: dim_estabelecimento (CNES — ADR-0006)
CREATE TABLE IF NOT EXISTS gold_dw.dim_estabelecimento (
    sk_estabelecimento SERIAL  PRIMARY KEY,
    co_cnes           INT     NOT NULL,
    nome_fantasia     VARCHAR(120),
    razao_social      VARCHAR(120),
    tipo_gestao       VARCHAR(10),
    co_municipio      INT,
    co_uf             SMALLINT,
    snapshot_date     DATE,
    dt_inicio         DATE    NOT NULL DEFAULT CURRENT_DATE,
    dt_fim            DATE    NOT NULL DEFAULT '9999-12-31',
    is_current        BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_estab_cnes ON gold_dw.dim_estabelecimento (co_cnes, is_current);

-- Fato Atendimento (streaming — agregado por batch job ou Spark foreachBatch)
-- Dados primários vivem em Delta Lake (ADR-0011); esta tabela serve de view materializada
-- para queries relacionais e referential integrity checks no smoke test suite.
CREATE TABLE IF NOT EXISTS gold_dw.fato_atendimento (
    sk_atendimento     BIGSERIAL    PRIMARY KEY,
    sk_paciente        INT          REFERENCES gold_dw.dim_paciente(sk_paciente),
    sk_estabelecimento INT          REFERENCES gold_dw.dim_estabelecimento(sk_estabelecimento),
    sk_tempo           INT          NOT NULL,
    tipo_atendimento   VARCHAR(50),
    triagem            VARCHAR(30),
    evento_ts          TIMESTAMPTZ  NOT NULL,
    kafka_offset       BIGINT,
    kafka_partition    SMALLINT,
    _batch_id          VARCHAR(64),
    _load_ts           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_atend_paciente ON gold_dw.fato_atendimento (sk_paciente);
CREATE INDEX IF NOT EXISTS idx_fato_atend_tempo    ON gold_dw.fato_atendimento (sk_tempo);
