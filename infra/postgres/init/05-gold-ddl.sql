-- Gold DW: Star Schema — DDL
-- Tabelas criadas aqui com IF NOT EXISTS; dados carregados por jobs Spark Gold.

SET search_path = gold_dw;

-- ── Dimensões ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_dw.dim_tempo (
    data          DATE        PRIMARY KEY,
    ano           SMALLINT    NOT NULL,
    mes           SMALLINT    NOT NULL,
    dia           SMALLINT    NOT NULL,
    semana_epidemiologica SMALLINT,
    trimestre     SMALLINT
);

CREATE TABLE IF NOT EXISTS gold_dw.dim_municipio (
    co_municipio  INT         PRIMARY KEY,
    nome_municipio VARCHAR(120),
    sigla_uf      CHAR(2),
    nome_uf       VARCHAR(60),
    co_uf         SMALLINT,
    _batch_id     VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS gold_dw.dim_agravo (
    id_agravo     VARCHAR(10) PRIMARY KEY,
    nome_agravo   VARCHAR(80) NOT NULL,
    categoria     VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS gold_dw.dim_vacina (
    co_vacina     INT         PRIMARY KEY,
    nome_vacina   VARCHAR(120),
    grupo_atendimento VARCHAR(80)
);

-- SCD Type 2: dim_estabelecimento
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

-- SCD Type 2: dim_paciente (apenas hash, sem PII — ADR-0004 + ADR-0006)
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

-- ── Fatos ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_dw.fato_notificacao (
    id              BIGSERIAL   PRIMARY KEY,
    id_agravo       VARCHAR(10),
    dt_notific      DATE,
    sk_tempo        INT,
    co_municipio    INT,
    nu_idade_n      SMALLINT,
    cs_sexo         CHAR(1),
    classi_fin      VARCHAR(4),
    evolucao        VARCHAR(4),
    hospitaliz      CHAR(1),
    agravo          VARCHAR(20) NOT NULL,
    ano_mes         CHAR(6)     NOT NULL,
    qtd_casos       SMALLINT    DEFAULT 1,
    _batch_id       VARCHAR(64),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_not_municipio ON gold_dw.fato_notificacao (co_municipio);
CREATE INDEX IF NOT EXISTS idx_fato_not_agravo    ON gold_dw.fato_notificacao (agravo, ano_mes);
CREATE INDEX IF NOT EXISTS idx_fato_not_tempo     ON gold_dw.fato_notificacao (sk_tempo);

CREATE TABLE IF NOT EXISTS gold_dw.fato_obito (
    id              BIGSERIAL   PRIMARY KEY,
    dt_obito        DATE,
    sk_tempo        INT,
    co_municipio_ocor INT,
    id_agravo       VARCHAR(10),
    idade           SMALLINT,
    cs_sexo         CHAR(1),
    ano_part        CHAR(4)     NOT NULL,
    qtd_obitos      SMALLINT    DEFAULT 1,
    _batch_id       VARCHAR(64),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_obito_municipio ON gold_dw.fato_obito (co_municipio_ocor);
CREATE INDEX IF NOT EXISTS idx_fato_obito_tempo     ON gold_dw.fato_obito (sk_tempo);

CREATE TABLE IF NOT EXISTS gold_dw.fato_vacinacao (
    id              BIGSERIAL   PRIMARY KEY,
    data_vacina     DATE,
    sk_tempo        INT,
    co_municipio_paciente INT,
    co_vacina       INT,
    co_estabelecimento INT,
    codigo_paciente VARCHAR(64),
    ano_mes         CHAR(6)     NOT NULL,
    qtd_doses       SMALLINT    DEFAULT 1,
    _batch_id       VARCHAR(64),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_vac_municipio ON gold_dw.fato_vacinacao (co_municipio_paciente);
CREATE INDEX IF NOT EXISTS idx_fato_vac_tempo     ON gold_dw.fato_vacinacao (sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_vac_vacina    ON gold_dw.fato_vacinacao (co_vacina);

-- Streaming replay — eventos do Kafka reprocessados em micro-batches
CREATE TABLE IF NOT EXISTS gold_dw.fato_atendimento_stream (
    id              BIGSERIAL   PRIMARY KEY,
    evento_ts       TIMESTAMPTZ NOT NULL,
    sk_tempo        INT,
    id_paciente_hash VARCHAR(64),
    agravo          VARCHAR(20),
    co_municipio    INT,
    tipo_atendimento VARCHAR(40),
    _kafka_offset   BIGINT,
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_stream_ts ON gold_dw.fato_atendimento_stream (evento_ts);

-- Batch fact table for atendimentos (joins with dim_paciente via sk_paciente)
CREATE TABLE IF NOT EXISTS gold_dw.fato_atendimento (
    id              BIGSERIAL   PRIMARY KEY,
    sk_paciente     INT,
    sk_tempo        INT,
    co_municipio    INT,
    tipo_atendimento VARCHAR(40),
    cid10_principal VARCHAR(10),
    agravo          VARCHAR(20),
    dt_atendimento  DATE,
    ano_mes         CHAR(6)     NOT NULL,
    qtd_atendimentos SMALLINT   DEFAULT 1,
    _batch_id       VARCHAR(64),
    _load_ts        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fato_atend_paciente ON gold_dw.fato_atendimento (sk_paciente);
CREATE INDEX IF NOT EXISTS idx_fato_atend_tempo    ON gold_dw.fato_atendimento (sk_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_atend_municipio ON gold_dw.fato_atendimento (co_municipio);
