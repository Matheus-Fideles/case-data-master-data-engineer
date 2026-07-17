-- Hive Metastore backend — usuário e banco dedicados
-- Executado antes do container hive-metastore iniciar (depende do postgres healthy)

CREATE USER hive WITH PASSWORD 'hive';
CREATE DATABASE hive OWNER hive;
GRANT ALL PRIVILEGES ON DATABASE hive TO hive;
