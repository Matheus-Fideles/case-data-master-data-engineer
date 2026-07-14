-- Seed determinístico para oltp.paciente (seed=42, 100 registros)
-- Gerado em 2026-07-13 — não editar manualmente.
-- Usado por: tests/smoke/conftest.py::seed_oltp
-- Compatível com: infra/postgres/init/00-oltp-ddl.sql

CREATE SCHEMA IF NOT EXISTS oltp;

CREATE TABLE IF NOT EXISTS oltp.paciente (
    id_paciente BIGSERIAL PRIMARY KEY,
    cpf         CHAR(11)     NOT NULL UNIQUE,
    nome        VARCHAR(120) NOT NULL,
    data_nascimento DATE     NOT NULL,
    sexo        CHAR(1)      NOT NULL CHECK (sexo IN ('M','F','O')),
    cep         CHAR(8)      NOT NULL,
    municipio_codigo_ibge CHAR(7) NOT NULL,
    email       VARCHAR(120),
    telefone    VARCHAR(20),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO oltp.paciente
    (cpf, nome, data_nascimento, sexo, cep, municipio_codigo_ibge, email, telefone)
VALUES
    ('12345678909', 'PACIENTE 0001 SILVA', '1950-01-01', 'M', '10000000', '3550308', 'paciente1@example.com', '11900000000'),
    ('23456789000', 'PACIENTE 0002 SILVA', '1951-02-02', 'F', '10000003', '3550309', 'paciente2@example.com', '11900000001'),
    ('34567890100', 'PACIENTE 0003 SILVA', '1952-03-03', 'M', '10000006', '3550310', 'paciente3@example.com', '11900000002'),
    ('45678901200', 'PACIENTE 0004 SILVA', '1953-04-04', 'F', '10000009', '3550311', 'paciente4@example.com', '11900000003'),
    ('56789012300', 'PACIENTE 0005 SILVA', '1954-05-05', 'M', '10000012', '3550312', 'paciente5@example.com', '11900000004'),
    ('67890123400', 'PACIENTE 0006 SILVA', '1955-06-06', 'F', '10000015', '3550313', 'paciente6@example.com', '11900000005'),
    ('78901234500', 'PACIENTE 0007 SILVA', '1956-07-07', 'M', '10000018', '3550314', 'paciente7@example.com', '11900000006'),
    ('89012345600', 'PACIENTE 0008 SILVA', '1957-08-08', 'F', '10000021', '3550315', 'paciente8@example.com', '11900000007'),
    ('90123456700', 'PACIENTE 0009 SILVA', '1958-09-09', 'M', '10000024', '3550316', 'paciente9@example.com', '11900000008'),
    ('01234567800', 'PACIENTE 0010 SILVA', '1959-10-10', 'F', '10000027', '3550317', 'paciente10@example.com', '11900000009'),
    ('11234567890', 'PACIENTE 0011 SILVA', '1960-11-11', 'M', '10000030', '3550318', 'paciente11@example.com', '11900000010'),
    ('22345678901', 'PACIENTE 0012 SILVA', '1961-12-12', 'F', '10000033', '3550319', 'paciente12@example.com', '11900000011'),
    ('33456789012', 'PACIENTE 0013 SILVA', '1962-01-13', 'M', '10000036', '3550320', 'paciente13@example.com', '11900000012'),
    ('44567890123', 'PACIENTE 0014 SILVA', '1963-02-14', 'F', '10000039', '3550321', 'paciente14@example.com', '11900000013'),
    ('55678901234', 'PACIENTE 0015 SILVA', '1964-03-15', 'M', '10000042', '3550322', 'paciente15@example.com', '11900000014'),
    ('66789012345', 'PACIENTE 0016 SILVA', '1965-04-16', 'F', '10000045', '3550323', 'paciente16@example.com', '11900000015'),
    ('77890123456', 'PACIENTE 0017 SILVA', '1966-05-17', 'M', '10000048', '3550324', 'paciente17@example.com', '11900000016'),
    ('88901234567', 'PACIENTE 0018 SILVA', '1967-06-18', 'F', '10000051', '3550325', 'paciente18@example.com', '11900000017'),
    ('99012345678', 'PACIENTE 0019 SILVA', '1968-07-19', 'M', '10000054', '3550326', 'paciente19@example.com', '11900000018'),
    ('10123456789', 'PACIENTE 0020 SILVA', '1969-08-20', 'F', '10000057', '3550327', 'paciente20@example.com', '11900000019')
ON CONFLICT (cpf) DO NOTHING;
