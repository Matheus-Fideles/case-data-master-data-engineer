"""Gera resumo-executivo.pdf do Case Santander - Vigilância Epidemiológica."""

from fpdf import FPDF
from fpdf.enums import XPos, YPos
from datetime import date
import os

GITHUB = "https://github.com/Matheus-Fideles/case-data-master-data-engineer"
AUTHOR = "Matheus Fideles Martins de Souza"
TODAY = date.today().strftime("%d/%m/%Y")

RED = (204, 10, 30)
DARK = (15, 23, 42)
GRAY = (71, 85, 105)
LIGHT = (248, 250, 252)
WHITE = (255, 255, 255)
ACCENT = (239, 68, 68)

FONT_DIR = "/System/Library/Fonts/Supplemental"
ARIAL = os.path.join(FONT_DIR, "Arial.ttf")
ARIAL_B = os.path.join(FONT_DIR, "Arial Bold.ttf")
ARIAL_I = os.path.join(FONT_DIR, "Arial Italic.ttf")
MONACO = "/System/Library/Fonts/Monaco.ttf"


class PDF(FPDF):
    def header(self):
        pass

    def _f(self, style="", size=10):
        """Set Arial font with given style and size."""
        if style == "B":
            self.set_font("ArialB", size=size)
        elif style == "I":
            self.set_font("ArialI", size=size)
        elif style == "M":
            self.set_font("Monaco", size=size)
        else:
            self.set_font("Arial", size=size)

    def footer(self):
        self.set_y(-14)
        self._f("I", 8)
        self.set_text_color(*GRAY)
        self.cell(0, 5, f"Case Santander · Vigilância Epidemiológica · {GITHUB}", align="C",
                  new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.cell(0, 5, f"Página {self.page_no()} · Gerado em {TODAY}", align="C")

    def cover(self):
        # Red header bar
        self.set_fill_color(*RED)
        self.rect(0, 0, 210, 80, "F")

        # Santander wordmark area
        self.set_xy(0, 18)
        self._f("B", 36)
        self.set_text_color(*WHITE)
        self.cell(0, 14, "Santander", align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self._f("", 13)
        self.set_text_color(255, 200, 200)
        self.cell(0, 8, "Academia de Dados · Data Master", align="C")

        # Title block
        self.set_xy(20, 92)
        self._f("B", 24)
        self.set_text_color(*DARK)
        self.multi_cell(170, 10, "Plataforma de Vigilância\nEpidemiológica - Resumo Executivo",
                        align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

        self.ln(4)
        self.set_x(20)
        self._f("", 12)
        self.set_text_color(*GRAY)
        self.multi_cell(170, 7,
            "Pipeline de dados end-to-end para ingestão, processamento e análise "
            "de notificações epidemiológicas do DataSUS, usando arquitetura medallion "
            "(Bronze -> Silver -> Gold) sobre Delta Lake em Kubernetes.",
            align="C")

        self.ln(10)
        # Divider
        self.set_draw_color(*RED)
        self.set_line_width(0.6)
        self.line(20, self.get_y(), 190, self.get_y())
        self.ln(6)

        # Meta row
        self._f("", 10)
        self.set_text_color(*GRAY)
        self.set_x(20)
        self.cell(55, 6, f"Autor: {AUTHOR}")
        self.cell(55, 6, f"Data: {TODAY}")
        self.cell(80, 6, f"GitHub: {GITHUB}", link=GITHUB)
        self.ln(16)

        # KPI boxes
        kpis = [
            ("17", "DAGs Airflow"),
            ("3", "Camadas Medallion"),
            ("7", "Tabelas Gold"),
            ("106", "Testes Unitários"),
            ("36", "Testes de Fumaça"),
        ]
        box_w = 32
        gap = 4
        total = len(kpis) * box_w + (len(kpis) - 1) * gap
        start_x = (210 - total) / 2
        y0 = self.get_y()
        for i, (val, label) in enumerate(kpis):
            x = start_x + i * (box_w + gap)
            self.set_fill_color(*LIGHT)
            self.set_draw_color(*RED)
            self.set_line_width(0.3)
            self.rect(x, y0, box_w, 22, "FD")
            self.set_xy(x, y0 + 2)
            self._f("B", 18)
            self.set_text_color(*RED)
            self.cell(box_w, 10, val, align="C", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            self.set_x(x)
            self._f("", 7)
            self.set_text_color(*GRAY)
            self.cell(box_w, 5, label, align="C")

    def section_title(self, text, icon=""):
        self.ln(6)
        self.set_fill_color(*RED)
        self.rect(self.l_margin, self.get_y(), 4, 7, "F")
        self.set_xy(self.l_margin + 6, self.get_y())
        self._f("B", 13)
        self.set_text_color(*DARK)
        self.cell(0, 7, f"{icon}  {text}" if icon else text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        self.ln(2)

    def bullet(self, text, indent=6):
        self.set_x(self.l_margin + indent)
        self._f("", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5.5, f"• {text}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def sub_title(self, text):
        self.set_x(self.l_margin)
        self._f("B", 10)
        self.set_text_color(*GRAY)
        self.cell(0, 6, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def body(self, text):
        self.set_x(self.l_margin)
        self._f("", 10)
        self.set_text_color(*DARK)
        self.multi_cell(0, 5.5, text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def table_row(self, cols, widths, bold=False, fill=False):
        if fill:
            self.set_fill_color(*LIGHT)
        style = "B" if bold else ""
        self._f(style, 9)
        self.set_text_color(*DARK)
        x0 = self.l_margin
        y0 = self.get_y()
        for text, w in zip(cols, widths):
            self.set_xy(x0, y0)
            self.cell(w, 6, str(text), border=1, fill=fill, new_x=XPos.RIGHT, new_y=YPos.TOP)
            x0 += w
        self.ln(6)


def build():
    pdf = PDF(orientation="P", unit="mm", format="A4")
    pdf.add_font("Arial", style="", fname=ARIAL)
    pdf.add_font("ArialB", style="", fname=ARIAL_B)
    pdf.add_font("ArialI", style="", fname=ARIAL_I)
    pdf.add_font("Monaco", style="", fname=MONACO)
    pdf.set_margins(20, 20, 20)
    pdf.set_auto_page_break(True, margin=18)
    pdf.add_page()

    # ── Capa ────────────────────────────────────────────────────────────────
    pdf.cover()

    # ── Pág 2: Contexto e Objetivo ─────────────────────────────────────────
    pdf.add_page()

    pdf.section_title("1. Contexto e Objetivo")
    pdf.body(
        "O Brasil notifica mais de 1,2 milhão de óbitos anuais via SIM (Sistema de Informação sobre "
        "Mortalidade) e dezenas de milhares de internações via SIH. Esses dados, disponibilizados "
        "pelo DataSUS em formato FTP/CSV, carecem de uma camada analítica acessível para gestores "
        "de saúde tomarem decisões em tempo hábil.\n\n"
        "Este case demonstra a construção de uma plataforma de dados end-to-end capaz de:"
    )
    bullets = [
        "Ingerir dados epidemiológicos brutos do DataSUS (SIM, SIH) via API REST e SFTP offline.",
        "Aplicar mascaramento de PII (CPF/nome) com SHA-256 + salt, em conformidade com a LGPD.",
        "Transformar e qualificar os dados em arquitetura medallion (Bronze -> Silver -> Gold).",
        "Expor tabelas Gold via Trino/Delta Lake para consultas analíticas em Metabase.",
        "Monitorar toda a plataforma com Prometheus, Grafana e rastreabilidade de linhagem via Marquez/OpenLineage.",
    ]
    for b in bullets:
        pdf.bullet(b)

    pdf.section_title("2. Arquitetura da Solução")
    pdf.body(
        "A plataforma adota o padrão Medallion em três camadas sobre Delta Lake armazenado no MinIO "
        "(S3-compatible). O processamento é feito por Spark 3.5 rodando via DockerOperator (local[2]) "
        "na rede Docker, orquestrado pelo Airflow (ADR-0012)."
    )

    layers = [
        ("Bronze", "Dados brutos do DataSUS. Schema On Read. Idempotente por partição data_ref.",
         "s3a://bronze/hospitalar/{fonte}/{data_ref}/"),
        ("Silver", "PII mascarada. Cast de tipos. Datas corrigidas (CORRECTED parser). Delta particionado.",
         "s3a://silver/hospitalar/{fonte}/"),
        ("Gold", "Star schema dimensional. KPIs calculados. Partição por ano/mes. Prontos para BI.",
         "s3a://gold/hospitalar/{tabela}/"),
    ]
    colors = [(180, 120, 40), (160, 160, 160), (210, 170, 0)]
    for (layer, desc, path), color in zip(layers, colors):
        self_y = pdf.get_y() + 2
        pdf.set_fill_color(*color)
        pdf.rect(pdf.l_margin, self_y, 3, 18, "F")
        pdf.set_xy(pdf.l_margin + 6, self_y)
        pdf._f("B", 11)
        pdf.set_text_color(*DARK)
        pdf.cell(30, 6, layer, new_x=XPos.RIGHT, new_y=YPos.TOP)
        pdf.set_x(pdf.l_margin + 6)
        pdf._f("", 9)
        pdf.set_text_color(*GRAY)
        pdf.set_xy(pdf.l_margin + 6, self_y + 6)
        pdf.multi_cell(160, 5, desc, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.set_x(pdf.l_margin + 6)
        pdf._f("I", 8)
        pdf.set_text_color(100, 100, 150)
        pdf.cell(0, 5, path, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(2)

    # ── Pág 3: Stack Tecnológica ───────────────────────────────────────────
    pdf.add_page()
    pdf.section_title("3. Stack Tecnológica")

    stack = [
        ("Componente", "Tecnologia", "Versão", "Papel"),
        ("Orquestração", "Apache Airflow", "2.9.3", "17 DAGs; DockerOperator (Spark local[2])"),
        ("Processamento", "Apache Spark", "3.5", "DockerOperator na rede Docker (lake)"),
        ("Data Lake", "Delta Lake + MinIO", "3.x / 2024-07", "Storage S3-compat + ACID"),
        ("Catálogo", "Hive Metastore", "3.1.3", "Metastore para Trino"),
        ("Query Engine", "Trino", "448", "SQL federado sobre Delta"),
        ("BI", "Metabase", "0.51.4", "Dashboards para gestores"),
        ("Mensageria", "Apache Kafka", "7.6.1 (KRaft)", "Tópico notificacoes.raw"),
        ("Linhagem", "Marquez / OpenLineage", "0.47.0", "Rastreabilidade de datasets"),
        ("Monitoramento", "Prometheus + Grafana", "2.52 / 10.4", "Métricas infra e pipeline"),
        ("IaC", "Docker Compose", "v2", "4 perfis: core/streaming/serving/obs"),
    ]
    widths = [38, 42, 30, 60]
    for i, row in enumerate(stack):
        pdf.table_row(row, widths, bold=(i == 0), fill=(i % 2 == 0))

    pdf.section_title("4. Decisões de Arquitetura (ADRs)")
    adrs = [
        ("ADR-0001", "Spark como motor de transformação", "Performance, Delta Lake nativo, PySpark testável"),
        ("ADR-0002", "Delta Lake como formato de armazenamento", "ACID, time-travel, schema evolution"),
        ("ADR-0003", "OFFLINE_MODE para desenvolvimento", "Reprodutibilidade sem depender do DataSUS"),
        ("ADR-0004", "Mascaramento SHA-256 + salt", "LGPD Art. 12 - dado anonimizado não é dado pessoal"),
        ("ADR-0005", "Medallion em 3 camadas", "Separação de responsabilidades, reprocessamento seguro"),
        ("ADR-0006", "Airflow como orquestrador", "Retry nativo, SLA tracking, integração OpenLineage"),
        ("ADR-0007", "Spark on Kubernetes (k3s)", "Isolamento de recursos, submissão declarativa via CRD"),
        ("ADR-0008", "Trino sobre Delta Lake", "SQL padrão sem mover dados, federação multi-catálogo"),
        ("ADR-0009", "OpenLineage / Marquez", "Rastreabilidade de linhagem, auditoria de datasets"),
    ]
    widths2 = [22, 68, 80]
    pdf.table_row(("ADR", "Decisão", "Justificativa"), widths2, bold=True, fill=True)
    for i, row in enumerate(adrs):
        pdf.table_row(row, widths2, fill=(i % 2 == 0))

    # ── Pág 4: Pipeline e Qualidade ───────────────────────────────────────
    pdf.add_page()
    pdf.section_title("5. Pipeline de Dados - 17 DAGs")

    dag_groups = [
        ("Bronze (Ingestão)", [
            "dag_bronze_sim_obitos - SIM: óbitos DataSUS",
            "dag_bronze_sih_aih - SIH: internações (AIH)",
            "dag_bronze_sivep_gripe - SIVEP: vigilância gripal",
            "dag_bronze_sinan_dengue - SINAN: notificações dengue",
            "dag_bronze_ibge_populacao - IBGE: estimativas populacionais",
            "dag_bronze_notificacoes_stream - Kafka consumer -> Bronze",
        ]),
        ("Silver (Qualidade & PII)", [
            "dag_silver_sim_obitos - Parse datas, máscaras PII, tipos",
            "dag_silver_sih_aih - Normalização procedimentos/diagnósticos",
            "dag_silver_sivep_gripe - Enriquecimento semana epidemiológica",
            "dag_silver_sinan_dengue - Dedup, tipagem, coordenadas",
        ]),
        ("Gold (Analítico)", [
            "dag_gold_dim_tempo - Dimensão calendário epidemiológico",
            "dag_gold_dim_cid - Dimensão CID-10",
            "dag_gold_dim_municipio - Dimensão IBGE municípios",
            "dag_gold_dim_estabelecimento - Dimensão CNES",
            "dag_gold_fato_obito - Fato óbitos (SIM × dims)",
            "dag_gold_fato_internacao - Fato internações (SIH × dims)",
            "dag_gold_kpi_mortalidade - KPI taxa mortalidade por 100k hab",
        ]),
        ("Qualidade", [
            "dag_quality_checks - Great Expectations: contratos de dados",
        ]),
    ]
    for group, dags in dag_groups:
        pdf.sub_title(group)
        for d in dags:
            pdf.bullet(d)
        pdf.ln(1)

    pdf.section_title("6. Qualidade e Testes")
    pdf.body(
        "A suite de testes cobre todas as camadas do pipeline com isolamento por tipo:"
    )
    test_rows = [
        ("Tipo", "Qtd", "Escopo"),
        ("Unitários (pytest)", "106", "Transformações Spark, mascaramento PII, parsers"),
        ("Fumaça (smoke)", "36", "Conectividade infra: MinIO, Postgres, Airflow, Marquez"),
        ("Qualidade (GX)", "-", "Contratos de schema, null rates, distribuições"),
        ("Segurança (bandit)", "0 issues", "SAST estático em apps/ e airflow/"),
    ]
    widths3 = [60, 20, 90]
    pdf.table_row(test_rows[0], widths3, bold=True, fill=True)
    for i, row in enumerate(test_rows[1:]):
        pdf.table_row(row, widths3, fill=(i % 2 == 0))

    pdf.ln(4)
    pdf.body(
        "CI/CD: GitHub Actions executa ruff (format + lint), pytest, bandit e build de imagens "
        "a cada push. Todos os 142 testes passam no pipeline (commits 3784c23 e f2ef66b)."
    )

    # ── Pág 5: LGPD e Observabilidade ─────────────────────────────────────
    pdf.add_page()
    pdf.section_title("7. Governança e LGPD")
    pdf.body(
        "O tratamento de dados pessoais segue a Lei Geral de Proteção de Dados (Lei 13.709/2018). "
        "A estratégia de anonimização aplicada é SHA-256 com salt variável, garantindo que o dado "
        "resultante não permita reidentificação direta (Art. 12 LGPD)."
    )
    lgpd_items = [
        "PII_SALT armazenado em Kubernetes Secret - nunca em código ou variáveis estáticas.",
        "Mascaramento aplicado na camada Silver, antes de qualquer escrita persistente.",
        "Campos afetados: CPF, nome_paciente, nome_mae - substituídos por hash hex de 64 chars.",
        "Acesso ao MinIO controlado por credenciais dedicadas (MINIO_ROOT_USER/PASSWORD).",
        "Audit trail via OpenLineage: toda transformação gera evento de linhagem rastreável.",
        "Dados brutos (Bronze) acessíveis apenas ao pipeline - sem exposição via SQL/BI.",
    ]
    for item in lgpd_items:
        pdf.bullet(item)

    pdf.section_title("8. Observabilidade")
    pdf.body(
        "A stack de observabilidade cobre métricas de infraestrutura, pipeline e data quality:"
    )
    obs = [
        ("Prometheus :9090", "Coleta métricas de cAdvisor (containers), MinIO (buckets/objetos) e self-monitoring"),
        ("Grafana :3000", "Dashboard Pipeline Overview: CPU, memória, rede, capacidade MinIO, status UP/DOWN"),
        ("cAdvisor :8088", "Exporta métricas Docker em tempo real para Prometheus"),
        ("Marquez Web :5012", "UI de linhagem OpenLineage - grafo de datasets e jobs"),
        ("Airflow :8080", "UI de orquestração - status de DAGs, logs de tarefas, SLA tracking"),
        ("Kafka UI :8090", "Monitoramento de tópicos, consumer groups e lag"),
    ]
    widths4 = [45, 125]
    pdf.table_row(("Serviço", "Função"), widths4, bold=True, fill=True)
    for i, row in enumerate(obs):
        pdf.table_row(row, widths4, fill=(i % 2 == 0))

    pdf.section_title("9. Como Executar")
    steps = [
        "git clone https://github.com/Matheus-Fideles/case-data-master-data-engineer",
        "cp .env.example .env  # configurar credenciais",
        "make up-core          # Postgres + MinIO + Airflow",
        "make up-all           # todos os profiles (streaming + serving + observability)",
        "# Acesse: Airflow :8080 · MinIO :9001 · Grafana :3000 · Marquez :5012",
        "# Trigger: Airflow UI -> dag_bronze_sim_obitos -> trigger w/ config",
        "make test             # 106 unit + 36 smoke tests",
    ]
    pdf._f("M", 9)
    pdf.set_fill_color(240, 240, 240)
    for step in steps:
        pdf.set_x(pdf.l_margin)
        pdf.set_text_color(40, 40, 80)
        pdf.cell(0, 5.5, step, fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(2)

    pdf.section_title("10. Repositório e Contato")
    pdf._f("", 10)
    pdf.set_text_color(*DARK)
    pdf.set_x(pdf.l_margin)
    pdf.cell(25, 6, "GitHub:")
    pdf.set_text_color(*RED)
    pdf.cell(0, 6, GITHUB, link=GITHUB, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_text_color(*DARK)
    pdf.set_x(pdf.l_margin)
    pdf.cell(25, 6, "Autor:")
    pdf.cell(0, 6, AUTHOR, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(pdf.l_margin)
    pdf.cell(25, 6, "E-mail:")
    pdf.cell(0, 6, "matheuss.fideles@hotmail.com", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_x(pdf.l_margin)
    pdf.cell(25, 6, "Data:")
    pdf.cell(0, 6, TODAY)

    out = "docs/resumo-executivo.pdf"
    pdf.output(out)
    print(f"PDF gerado: {out}")


if __name__ == "__main__":
    build()
