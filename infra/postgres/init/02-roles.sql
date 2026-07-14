-- Roles e permissões por schema
-- Ver: docs/standards/pre-commit.md e docs/architecture/decisions/0004-pii-masking.md

-- ── Roles ──────────────────────────────────────────────────────────────────

-- Airflow usa o superuser postgres para gerenciar seu próprio schema
-- Marquez idem

-- Role para jobs Spark (escrita no Gold DW)
DO $$ BEGIN
  CREATE ROLE gold_engineer LOGIN PASSWORD 'gold_engineer';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Role para analistas (somente leitura no Gold DW)
DO $$ BEGIN
  CREATE ROLE gold_analyst LOGIN PASSWORD 'gold_analyst';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Role para Metabase (somente leitura em views específicas)
DO $$ BEGIN
  CREATE ROLE metabase LOGIN PASSWORD 'metabase';
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ── Permissões gold_dw ────────────────────────────────────────────────────
GRANT USAGE ON SCHEMA gold_dw TO gold_engineer, gold_analyst, metabase;
GRANT CREATE ON SCHEMA gold_dw TO gold_engineer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gold_dw TO gold_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA gold_dw TO gold_analyst;

ALTER DEFAULT PRIVILEGES IN SCHEMA gold_dw
  GRANT SELECT, INSERT, UPDATE ON TABLES TO gold_engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold_dw
  GRANT SELECT ON TABLES TO gold_analyst;

-- ── Permissões oltp ───────────────────────────────────────────────────────
-- gold_engineer pode ler oltp para o pipeline batch Bronze (OLTP snapshot)
GRANT USAGE ON SCHEMA oltp TO gold_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA oltp TO gold_engineer;
ALTER DEFAULT PRIVILEGES IN SCHEMA oltp
  GRANT SELECT ON TABLES TO gold_engineer;

-- ── Permissões Metabase ───────────────────────────────────────────────────
GRANT USAGE ON SCHEMA gold_dw TO metabase;
GRANT SELECT ON ALL TABLES IN SCHEMA gold_dw TO metabase;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold_dw
  GRANT SELECT ON TABLES TO metabase;
