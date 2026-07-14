"""Populates oltp.paciente with synthetic data via Faker.

Run via: make seed-oltp
Requires: Postgres running (docker compose up -d postgres)

Generates N_PACIENTES records with valid CPF (checksum), name, address
and synthetic pt_BR demographic data. Idempotent: uses INSERT ON
CONFLICT DO NOTHING, so it can be re-run without duplicating data.
"""

from __future__ import annotations

import os
import random
import sys

try:
    import psycopg2
    from faker import Faker
except ImportError:
    print("Install dependencies: pip install psycopg2-binary faker")
    sys.exit(1)

N_PACIENTES = int(os.environ.get("SEED_N_PACIENTES", "500"))
PG_DSN = os.environ.get(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=app user=postgres password=postgres",
)

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

MUNICIPIOS = [
    ("3550308", "01310100"),  # Sao Paulo - SP
    ("3304557", "20040020"),  # Rio de Janeiro - RJ
    ("3106200", "30130110"),  # Belo Horizonte - MG
    ("2927408", "40010010"),  # Salvador - BA
    ("2304400", "60135110"),  # Fortaleza - CE
    ("1302603", "69025010"),  # Manaus - AM
    ("4314902", "90010280"),  # Porto Alegre - RS
    ("4106902", "80010010"),  # Curitiba - PR
    ("2611606", "50010020"),  # Recife - PE
    ("5300108", "70040010"),  # Brasilia - DF
]


def _gerar_cpf() -> str:
    """Generates a CPF with valid check digits."""
    nums = [random.randint(0, 9) for _ in range(9)]

    soma = sum((10 - i) * n for i, n in enumerate(nums))
    d1 = 0 if (soma % 11) < 2 else 11 - (soma % 11)
    nums.append(d1)

    soma = sum((11 - i) * n for i, n in enumerate(nums))
    d2 = 0 if (soma % 11) < 2 else 11 - (soma % 11)
    nums.append(d2)

    return "".join(map(str, nums))


def _gerar_paciente() -> dict:
    municipio_ibge, cep = random.choice(MUNICIPIOS)
    sexo = random.choice(["M", "F", "O"])
    nome = fake.name_male() if sexo == "M" else fake.name_female() if sexo == "F" else fake.name()
    return {
        "cpf": _gerar_cpf(),
        "nome": nome,
        "data_nascimento": fake.date_of_birth(minimum_age=0, maximum_age=100).isoformat(),
        "sexo": sexo,
        "cep": cep,
        "municipio_codigo_ibge": municipio_ibge,
        "email": fake.email() if random.random() > 0.2 else None,
        "telefone": fake.phone_number()[:20] if random.random() > 0.3 else None,
    }


def main() -> None:
    print(f"Conectando ao Postgres: {PG_DSN}")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM oltp.paciente")
    existing = cur.fetchone()[0]
    if existing >= N_PACIENTES:
        print(f"oltp.paciente already has {existing} records — nothing to do.")
        conn.close()
        return

    print(f"Generating {N_PACIENTES} synthetic patients...")
    inseridos = 0
    cpfs_vistos: set[str] = set()

    while inseridos < N_PACIENTES:
        p = _gerar_paciente()
        if p["cpf"] in cpfs_vistos:
            continue
        cpfs_vistos.add(p["cpf"])

        cur.execute(
            """
            INSERT INTO oltp.paciente
                (cpf, nome, data_nascimento, sexo, cep, municipio_codigo_ibge, email, telefone)
            VALUES
                (%(cpf)s, %(nome)s, %(data_nascimento)s, %(sexo)s,
                 %(cep)s, %(municipio_codigo_ibge)s, %(email)s, %(telefone)s)
            ON CONFLICT (cpf) DO NOTHING
            """,
            p,
        )
        if cur.rowcount:
            inseridos += 1

        if inseridos % 100 == 0:
            conn.commit()
            print(f"  {inseridos}/{N_PACIENTES}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Seed complete: {inseridos} patients inserted into oltp.paciente.")


if __name__ == "__main__":
    main()
