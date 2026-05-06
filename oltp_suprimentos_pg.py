"""
╔══════════════════════════════════════════════════════════════════╗
║   Gerador de Base OLTP — Suprimentos + Financeiro                ║
║   (Simulação RM/TOTVS)                                           ║
║   Destino : PostgreSQL                                           ║
║   Modo    : INCREMENTAL + UPDATE de registros existentes         ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Cadastros / Dimensões:                                ║
║    ttmv          → Tipos de Movimento                            ║
║    fcfo          → Cadastro de Clientes/Fornecedores             ║
║    gccusto       → Centro de Custo                               ║
║    tloc          → Locais de Estoque (Almoxarifados)             ║
║    pproduto      → Produtos                                      ║
║    fctdo         → Tipos de Documento Financeiro (novo)          ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Movimentos / Transações:                              ║
║    tmov          → Cabeçalho dos Movimentos (SC / OC / REC)      ║
║    titmmov       → Itens dos Movimentos                          ║
║    tmovrelac     → Ponte entre cabeçalhos (SC→OC, OC→REC)        ║
║    titmmovrelac  → Ponte entre itens                             ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Financeiro (módulo expandido):                        ║
║    flan          → Lançamentos Financeiros (schema real RM)      ║
║    flanbaixa     → Baixas parciais e quitação por lançamento     ║
║    ftrblan       → Tributos por lançamento (ISSQN/IRPJ/INSS/     ║
║                    PIS/COFINS/CSLL/IOF)                          ║
╠══════════════════════════════════════════════════════════════════╣
║  MÓDULO FINANCEIRO — detalhes de simulação:                      ║
║                                                                  ║
║  flan                                                            ║
║    • Schema alinhado ao RM real: codcoligada, codfilial,         ║
║      codtdo, numerodocumento (NF/parcela ex: "000123/01"),       ║
║      datapag, dataprevbaixa, datacancelamento, datacancelbaixa,  ║
║      valorop1 (ISSQN), valorop2 (IRPJ), valorop3 (INSSPJ),      ║
║      valordesconto, valorjuros, valormulta, valorbaixado         ║
║    • ~5% dos lançamentos são cancelados (statuslan='C')          ║
║    • ~3% têm baixa cancelada após pagamento (datacancelbaixa)    ║
║                                                                  ║
║  flanbaixa                                                       ║
║    • Lançamentos pagos podem ter 1..3 baixas                     ║
║      – se 1 baixa: quitação direta                               ║
║      – se 2+ baixas: parciais distribuídas aleatoriamente,       ║
║        última completa o saldo                                   ║
║    • valorbaixado na flan = soma das baixas geradas              ║
║    • ~3% dos pagos têm datacancelbaixa (baixa cancelada)         ║
║                                                                  ║
║  ftrblan                                                         ║
║    • Tributos calculados por lançamento                          ║
║    • Incidência varia por codtdo (NF Serviço tributa mais)       ║
║    • Alíquotas simuladas: ISSQN 5%, IRPJ 1.5%, INSSPJ 11%,       ║
║      PIS 0.65%, COFINS 3%, CSLL 1%, IOF (só financeiro)          ║
║    • Uma linha por tributo por lançamento (codtributo)           ║
║                                                                  ║
║  fctdo                                                           ║
║    • Tipos de documento: NF Mercadoria, NF Serviço, Boleto,      ║
║      Duplicata, Recibo, Adiantamento, etc.                       ║
║    • codtdo '23' = estorno, excluído das análises                ║
╠══════════════════════════════════════════════════════════════════╣
║  ESTRUTURA DE ITENS — padrão RM:                                 ║
║    titmmov: nitem reinicia a cada movimento. PK real: (idmov, nitem) ║
║    iditmov: surrogate key apenas para facilitar JOINs            ║
║    titmmovrelac: rastreabilidade por idmov_orig + nitem_orig     ║
╠══════════════════════════════════════════════════════════════════╣
║  Diferenças em relação à versão SQL Server:                      ║
║    - Driver: psycopg2 (em vez de pyodbc)                         ║
║    - DATETIME2      → TIMESTAMP                                  ║
║    - BIT            → BOOLEAN                                    ║
║    - TINYINT        → SMALLINT  (PostgreSQL não tem TINYINT)     ║
║    - DECIMAL        → NUMERIC                                    ║
║    - TOP (n)        → LIMIT n                                    ║
║    - NEWID()        → RANDOM()                                   ║
║    - ISNULL()       → COALESCE()                                 ║
║    - GETDATE()      → CURRENT_DATE / NOW()                       ║
║    - Placeholders ? → %s                                         ║
║    - executemany    → execute_batch (psycopg2.extras)            ║
╠══════════════════════════════════════════════════════════════════╣
║  Comportamento a cada execução:                                  ║
║    1ª (DROP_RECREATE=True)  → cria e popula do zero              ║
║    2ª+ (DROP_RECREATE=False)→ insere novos + UPDATE 10% antigos  ║
╚══════════════════════════════════════════════════════════════════╝

Dependências:
    pip install psycopg2-binary faker python-dotenv
"""

import os
import psycopg2
import psycopg2.extras
import random
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════════
#  ▶  CONFIG
# ══════════════════════════════════════════════════════════════════

load_dotenv()

PG_HOST     = os.getenv("SUPABASE_HOST")
PG_PORT     = os.getenv("SUPABASE_PORT")
PG_DATABASE = os.getenv("SUPABASE_DBNAME")
PG_USER     = os.getenv("SUPABASE_USER")
PG_PWD      = os.getenv("SUPABASE_PASSWORD")

NUM_SCs        = 200
OCs_POR_SC     = (1, 3)
RECs_POR_OC    = (1, 3)
ITENS_POR_MOV  = (1, 5)
NUM_PRODUTOS   = 200
NUM_FORNEC     = 60
NUM_LOCAIS     = 8

OVERLAP_PERC   = 0.10
DROP_RECREATE  = False
BATCH_SIZE     = 2000
SEED           = None

# Coligada e filiais simuladas (padrão RM multi-empresa)
CODCOLIGADA    = 1
FILIAIS_CODIGO = [1, 2, 3, 4]   # int — codfilial na flan/flanbaixa

if SEED is not None:
    random.seed(SEED)
    Faker.seed(SEED)
fake = Faker("pt_BR")


# ══════════════════════════════════════════════════════════════════
#  1. TTMV
# ══════════════════════════════════════════════════════════════════
TTMV_DATA = [
    ("1.4.1", "SC Padrão",         "SC",  "Solicitação de compra padrão"),
    ("1.4.2", "SC Urgente",        "SC",  "Solicitação de compra com urgência"),
    ("1.5.3", "OC Nacional",       "OC",  "Pedido de compra fornecedor nacional"),
    ("1.5.4", "OC Importação",     "OC",  "Pedido de compra importação"),
    ("1.5.5", "OC Serviço",        "OC",  "Pedido de compra de serviço"),
    ("1.6.3", "REC NF Mercadoria", "REC", "Recebimento via nota fiscal de mercadoria"),
    ("1.6.4", "REC NF Serviço",    "REC", "Recebimento via nota fiscal de serviço"),
    ("1.6.5", "REC Parcial",       "REC", "Recebimento parcial do pedido"),
]
CODTMV_SC  = [r[0] for r in TTMV_DATA if r[2] == "SC"]
CODTMV_OC  = [r[0] for r in TTMV_DATA if r[2] == "OC"]
CODTMV_REC = [r[0] for r in TTMV_DATA if r[2] == "REC"]

# ══════════════════════════════════════════════════════════════════
#  2. FCTDO — Tipos de Documento Financeiro
# ══════════════════════════════════════════════════════════════════
# codtdo '23' = estorno — excluído nas análises (WHERE codtdo <> '23')
FCTDO_DATA = [
    ("01", "NF Mercadoria",    "C",  "Nota fiscal de mercadoria"),
    ("02", "NF Serviço",       "C",  "Nota fiscal de serviço"),
    ("03", "Boleto Bancário",  "C",  "Cobrança via boleto"),
    ("04", "Duplicata",        "C",  "Duplicata mercantil"),
    ("05", "Recibo",           "C",  "Recibo de pagamento"),
    ("06", "Adiantamento",     "C",  "Adiantamento a fornecedor"),
    ("07", "Contrato",         "C",  "Parcela de contrato"),
    ("08", "Devolução",        "D",  "Devolução de mercadoria"),
    ("09", "NF Importação",    "C",  "Nota fiscal de importação"),
    ("10", "Frete",            "C",  "Nota de frete/CTE"),
    ("23", "Estorno",          "E",  "Estorno de lançamento — excluído das análises"),
]

# codtdo associados a serviços (tributam ISSQN e INSSPJ)
CODTDO_SERVICO    = {"02", "05", "07"}
# codtdo onde IOF pode incidir
CODTDO_FINANCEIRO = {"06"}

# Pesos de sorteio para codtdo nos lançamentos de compra
CODTDO_COMPRA = ["01", "02", "03", "04", "05", "06", "07", "09", "10"]
CODTDO_PESOS  = [30,   15,   20,   15,    5,    5,    5,    3,    2  ]

# ══════════════════════════════════════════════════════════════════
#  3. DDL
#  Convenções PostgreSQL mantidas:
#    - Nomes de tabela/coluna em lowercase
#    - TIMESTAMP (sem fuso) em vez de DATETIME2
#    - BOOLEAN em vez de BIT
#    - SMALLINT em vez de TINYINT
#    - NUMERIC em vez de DECIMAL
#    - CREATE TABLE IF NOT EXISTS
#    - DROP ... CASCADE
# ══════════════════════════════════════════════════════════════════
DDL_TABLES = {

"ttmv": """CREATE TABLE IF NOT EXISTS ttmv (
    codtmv         VARCHAR(10)  NOT NULL,
    nome           VARCHAR(60)  NOT NULL,
    tipo           CHAR(3)      NOT NULL CHECK (tipo IN ('SC','OC','REC')),
    descricao      VARCHAR(200),
    dt_atualizacao TIMESTAMP    NOT NULL,
    CONSTRAINT pk_ttmv PRIMARY KEY (codtmv)
)""",

# ── fctdo — novo ─────────────────────────────────────────────────
"fctdo": """CREATE TABLE IF NOT EXISTS fctdo (
    codtdo         VARCHAR(5)   NOT NULL,
    descricao      VARCHAR(80)  NOT NULL,
    classific      CHAR(1)      NOT NULL CHECK (classific IN ('C','D','E')),
    observacao     VARCHAR(200),
    dt_atualizacao TIMESTAMP    NOT NULL,
    CONSTRAINT pk_fctdo PRIMARY KEY (codtdo)
)""",

"fcfo": """CREATE TABLE IF NOT EXISTS fcfo (
    codcfo         INT           NOT NULL,
    nomecfo        VARCHAR(120)  NOT NULL,
    tipocfo        CHAR(1)       NOT NULL CHECK (tipocfo IN ('C','F','A')),
    cnpj_cpf       VARCHAR(20),
    email          VARCHAR(100),
    telefone       VARCHAR(20),
    cidade         VARCHAR(60),
    uf             CHAR(2),
    ativo          BOOLEAN       NOT NULL DEFAULT TRUE,
    dt_atualizacao TIMESTAMP     NOT NULL,
    CONSTRAINT pk_fcfo PRIMARY KEY (codcfo)
)""",

"gccusto": """CREATE TABLE IF NOT EXISTS gccusto (
    codccusto      VARCHAR(20)  NOT NULL,
    nome           VARCHAR(80)  NOT NULL,
    tipo           VARCHAR(20),
    responsavel    VARCHAR(80),
    ativo          BOOLEAN      NOT NULL DEFAULT TRUE,
    dt_atualizacao TIMESTAMP    NOT NULL,
    CONSTRAINT pk_gccusto PRIMARY KEY (codccusto)
)""",

"tloc": """CREATE TABLE IF NOT EXISTS tloc (
    idloc          INT          NOT NULL,
    nome           VARCHAR(80)  NOT NULL,
    filial         VARCHAR(10),
    tipo           VARCHAR(20),
    ativo          BOOLEAN      NOT NULL DEFAULT TRUE,
    dt_atualizacao TIMESTAMP    NOT NULL,
    CONSTRAINT pk_tloc PRIMARY KEY (idloc)
)""",

"pproduto": """CREATE TABLE IF NOT EXISTS pproduto (
    codprod        INT           NOT NULL,
    nomeprod       VARCHAR(100)  NOT NULL,
    categoria      VARCHAR(40),
    unidade        VARCHAR(6),
    preco_base     NUMERIC(15,2),
    ativo          BOOLEAN       NOT NULL DEFAULT TRUE,
    dt_atualizacao TIMESTAMP     NOT NULL,
    CONSTRAINT pk_pproduto PRIMARY KEY (codprod)
)""",

"tmov": """CREATE TABLE IF NOT EXISTS tmov (
    idmov            INT           NOT NULL,
    codtmv           VARCHAR(10)   NOT NULL,
    numeromov        VARCHAR(20)   NOT NULL,
    dataemissao      DATE,
    dataentrega      DATE,
    datacompetencia  DATE,
    codfornec        INT,
    filial           VARCHAR(10),
    codccusto        VARCHAR(20),
    status           VARCHAR(20),
    valortotal       NUMERIC(15,2) DEFAULT 0,
    desconto         NUMERIC(15,2) DEFAULT 0,
    frete            NUMERIC(15,2) DEFAULT 0,
    observacao       VARCHAR(500),
    usuariocriacao   VARCHAR(40),
    datacriacao      TIMESTAMP,
    dt_atualizacao   TIMESTAMP     NOT NULL,
    CONSTRAINT pk_tmov      PRIMARY KEY (idmov),
    CONSTRAINT uq_numeromov UNIQUE (numeromov),
    CONSTRAINT fk_tmov_ttmv FOREIGN KEY (codtmv)    REFERENCES ttmv(codtmv),
    CONSTRAINT fk_tmov_fcfo FOREIGN KEY (codfornec) REFERENCES fcfo(codcfo)
)""",

"titmmov": """CREATE TABLE IF NOT EXISTS titmmov (
    iditmov        INT           NOT NULL,
    idmov          INT           NOT NULL,
    nitem          SMALLINT      NOT NULL,
    codprod        INT           NOT NULL,
    idloc          INT,
    quantidade     NUMERIC(15,3),
    valorunit      NUMERIC(15,2),
    valortotal     NUMERIC(15,2),
    unidade        VARCHAR(6),
    observacao     VARCHAR(300),
    dt_atualizacao TIMESTAMP     NOT NULL,
    CONSTRAINT pk_titmmov       PRIMARY KEY (iditmov),
    CONSTRAINT uq_titmmov_nitem UNIQUE (idmov, nitem),
    CONSTRAINT fk_titmmov_tmov  FOREIGN KEY (idmov)   REFERENCES tmov(idmov),
    CONSTRAINT fk_titmmov_prod  FOREIGN KEY (codprod) REFERENCES pproduto(codprod),
    CONSTRAINT fk_titmmov_loc   FOREIGN KEY (idloc)   REFERENCES tloc(idloc)
)""",

"tmovrelac": """CREATE TABLE IF NOT EXISTS tmovrelac (
    idmovrelac     INT       NOT NULL,
    idmov_orig     INT       NOT NULL,
    idmov_dest     INT       NOT NULL,
    tipo_orig      CHAR(3)   NOT NULL,
    tipo_dest      CHAR(3)   NOT NULL,
    dt_atualizacao TIMESTAMP NOT NULL,
    CONSTRAINT pk_tmovrelac      PRIMARY KEY (idmovrelac),
    CONSTRAINT fk_tmovrelac_orig FOREIGN KEY (idmov_orig) REFERENCES tmov(idmov),
    CONSTRAINT fk_tmovrelac_dest FOREIGN KEY (idmov_dest) REFERENCES tmov(idmov)
)""",

"titmmovrelac": """CREATE TABLE IF NOT EXISTS titmmovrelac (
    iditmmovrelac  INT       NOT NULL,
    idmov_orig     INT       NOT NULL,
    nitem_orig     SMALLINT  NOT NULL,
    idmov_dest     INT       NOT NULL,
    nitem_dest     SMALLINT  NOT NULL,
    dt_atualizacao TIMESTAMP NOT NULL,
    CONSTRAINT pk_titmmovrelac  PRIMARY KEY (iditmmovrelac),
    CONSTRAINT fk_itmrelac_orig FOREIGN KEY (idmov_orig, nitem_orig)
                                REFERENCES titmmov (idmov, nitem),
    CONSTRAINT fk_itmrelac_dest FOREIGN KEY (idmov_dest, nitem_dest)
                                REFERENCES titmmov (idmov, nitem)
)""",

# ── flan — schema expandido (alinhado ao RM real) ─────────────────
#
# Principais adições vs. versão anterior:
#   codcoligada        : empresa (multi-tenant, padrão RM)
#   codfilial          : filial de origem do lançamento
#   codtdo             : tipo de documento (FK para fctdo)
#   numerodocumento    : número NF + parcela ex: '000123/01'
#   dataemissao        : emissão do documento
#   dataprevbaixa      : previsão de baixa (agenda de tesouraria)
#   datapag            : data do pagamento efetivo
#   datacancelamento   : cancelamento do lançamento
#   datacancelbaixa    : cancelamento de uma baixa já realizada
#   valorop1 (ISSQN)   : dedução ISS sobre serviço
#   valorop2 (IRPJ)    : dedução IR na fonte
#   valorop3 (INSSPJ)  : dedução INSS PJ
#   valordesconto      : desconto concedido/obtido
#   valorjuros         : juros de mora
#   valormulta         : multa por atraso
#   valorbaixado       : soma acumulada de baixas (atualizado pela flanbaixa)
"flan": """CREATE TABLE IF NOT EXISTS flan (
    idlan              INT           NOT NULL,
    codcoligada        SMALLINT      NOT NULL DEFAULT 1,
    codfilial          SMALLINT      NOT NULL DEFAULT 1,
    idmov              INT,
    codfornec          INT,
    codtdo             VARCHAR(5),
    numerodocumento    VARCHAR(30),
    numparcela         SMALLINT      NOT NULL DEFAULT 1,
    totparcelas        SMALLINT      NOT NULL DEFAULT 1,
    pagrec             SMALLINT      NOT NULL DEFAULT 2,
    dataemissao        DATE,
    datavencimento     DATE          NOT NULL,
    dataprevbaixa      DATE,
    datapag            DATE,
    datacancelamento   DATE,
    datacancelbaixa    DATE,
    valororiginal      NUMERIC(15,2) NOT NULL,
    valorop1           NUMERIC(15,2) NOT NULL DEFAULT 0,
    valorop2           NUMERIC(15,2) NOT NULL DEFAULT 0,
    valorop3           NUMERIC(15,2) NOT NULL DEFAULT 0,
    valordesconto      NUMERIC(15,2) NOT NULL DEFAULT 0,
    valorjuros         NUMERIC(15,2) NOT NULL DEFAULT 0,
    valormulta         NUMERIC(15,2) NOT NULL DEFAULT 0,
    valorbaixado       NUMERIC(15,2) NOT NULL DEFAULT 0,
    statuslan          CHAR(1)       NOT NULL DEFAULT 'A',
    historico          VARCHAR(200),
    codccusto          VARCHAR(20),
    dt_atualizacao     TIMESTAMP     NOT NULL,
    CONSTRAINT pk_flan       PRIMARY KEY (idlan),
    CONSTRAINT fk_flan_tmov  FOREIGN KEY (idmov)      REFERENCES tmov(idmov),
    CONSTRAINT fk_flan_fcfo  FOREIGN KEY (codfornec)  REFERENCES fcfo(codcfo),
    CONSTRAINT fk_flan_fctdo FOREIGN KEY (codtdo)     REFERENCES fctdo(codtdo)
)""",

# ── flanbaixa — novo ──────────────────────────────────────────────
#
# Registra cada evento de baixa (parcial ou total) de um lançamento.
# Um lançamento pode ter 1..N baixas antes da quitação.
# valorbaixado: valor desta baixa específica (não acumulado)
"flanbaixa": """CREATE TABLE IF NOT EXISTS flanbaixa (
    idbaixa        INT           NOT NULL,
    idlan          INT           NOT NULL,
    codcoligada    SMALLINT      NOT NULL DEFAULT 1,
    codfilial      SMALLINT      NOT NULL DEFAULT 1,
    databaixa      DATE          NOT NULL,
    valorbaixado   NUMERIC(15,2) NOT NULL,
    historico      VARCHAR(200),
    dt_atualizacao TIMESTAMP     NOT NULL,
    CONSTRAINT pk_flanbaixa      PRIMARY KEY (idbaixa),
    CONSTRAINT fk_flanbaixa_flan FOREIGN KEY (idlan) REFERENCES flan(idlan)
)""",

# ── ftrblan — novo ────────────────────────────────────────────────
#
# Tributos retidos / calculados por lançamento financeiro.
# Cada linha = um tributo específico para um idlan.
#
# codtributo  : código do tributo (ISSQN, IRPJ, INSSPJ, PIS, COFINS, CSLL, IOF)
# valor       : valor calculado = aliquota × base_calculo
# aliquota    : alíquota aplicada (ex: 0.0500 = 5%)
# base_calculo: base sobre a qual o tributo foi calculado
#
# Incidência por tipo de documento (codtdo):
#   Serviço (02,05,07)  → ISSQN + IRPJ + INSSPJ + PIS + COFINS + CSLL
#   Mercadoria (01,04)  → PIS + COFINS + CSLL
#   Financeiro (06)     → IOF
#   Outros              → IRPJ + PIS + COFINS
"ftrblan": """CREATE TABLE IF NOT EXISTS ftrblan (
    idftrblan      INT            NOT NULL,
    idlan          INT            NOT NULL,
    codcoligada    SMALLINT       NOT NULL DEFAULT 1,
    codtributo     VARCHAR(10)    NOT NULL,
    descricao      VARCHAR(60),
    aliquota       NUMERIC(7,4)   NOT NULL,
    base_calculo   NUMERIC(15,2)  NOT NULL,
    valor          NUMERIC(15,2)  NOT NULL,
    dt_atualizacao TIMESTAMP      NOT NULL,
    CONSTRAINT pk_ftrblan      PRIMARY KEY (idftrblan),
    CONSTRAINT fk_ftrblan_flan FOREIGN KEY (idlan) REFERENCES flan(idlan)
)""",
}

# ══════════════════════════════════════════════════════════════════
#  4. Helpers gerais
# ══════════════════════════════════════════════════════════════════
CATEGORIAS = ["Matéria-Prima", "Embalagem", "MRO", "TI", "Serviço", "EPI", "Químico", "Logística"]
UNIDADES   = ["UN", "KG", "L", "M", "CX", "PC", "M2", "TON"]
FILIAIS    = ["0001", "0002", "0003", "0004"]
CENTROS    = ["CC001", "CC002", "CC003", "CC004", "CC005", "CC006"]
STATUS_SC  = ["Aberta", "Em Análise", "Aprovada", "Cancelada"]
STATUS_OC  = ["Emitida", "Parcial", "Encerrada", "Cancelada"]
STATUS_REC = ["Recebido", "Recebido Parcial", "Devolvido"]

TIPOS_CCUSTO = ["Operacional", "Administrativo", "Logística", "TI", "Comercial"]
TIPOS_LOC    = ["Matéria-Prima", "Produto Acabado", "MRO", "Expedição", "Quarentena"]

USUARIOS = [f"user.{fake.first_name().lower()}{random.randint(1,99):02d}" for _ in range(20)]

STATUS_FLOW = {
    "Aberta":           ["Em Análise", "Aprovada"],
    "Em Análise":       ["Aprovada", "Cancelada"],
    "Emitida":          ["Parcial", "Encerrada"],
    "Parcial":          ["Encerrada"],
    "Recebido Parcial": ["Recebido"],
}

def rand_date(days_ago_max: int = 365 * 2, days_ago_min: int = 60) -> datetime:
    return datetime.now() - timedelta(days=random.randint(days_ago_min, days_ago_max))

def competencia(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def nxt(c: list) -> int:
    c[0] += 1
    return c[0]

def proximo_status(atual: str) -> str:
    opcoes = STATUS_FLOW.get(atual)
    return random.choice(opcoes) if opcoes else atual

def sample_ids(cur, tabela: str, pk: str, n: int) -> tuple:
    cur.execute(f"SELECT {pk} FROM {tabela} ORDER BY RANDOM() LIMIT %s", (n,))
    return tuple(row[0] for row in cur.fetchall())

def num_nf(idlan: int) -> str:
    """Gera número de NF simulado (6 dígitos)."""
    base = (idlan * 7 + 100000) % 900000 + 100000
    return f"{base:06d}"


# ══════════════════════════════════════════════════════════════════
#  5. Lógica de tributos (ftrblan)
# ══════════════════════════════════════════════════════════════════

TRIBUTOS = {
    "ISSQN":  ("ISS sobre Serviços",           0.0500),
    "IRPJ":   ("IR Retido na Fonte",           0.0150),
    "INSSPJ": ("INSS Pessoa Jurídica",          0.1100),
    "PIS":    ("PIS/Pasep",                    0.0065),
    "COFINS": ("COFINS",                       0.0300),
    "CSLL":   ("Contribuição Social s/ Lucro", 0.0100),
    "IOF":    ("IOF",                          0.0038),
}

def tributos_por_codtdo(codtdo: str) -> list:
    if codtdo in CODTDO_SERVICO:
        return ["ISSQN", "IRPJ", "INSSPJ", "PIS", "COFINS", "CSLL"]
    elif codtdo in CODTDO_FINANCEIRO:
        return ["IOF"]
    elif codtdo in ("01", "04", "09"):
        return ["PIS", "COFINS", "CSLL"]
    else:
        return ["IRPJ", "PIS", "COFINS"]

def gerar_ftrblan(flan_rows: list, base_id: int) -> list:
    """
    Gera registros de tributos para cada lançamento da flan.
    ~70% dos lançamentos têm tributos.
    Alíquotas sofrem variação de ±20% para simular regimes diferenciados.
    """
    rows = []
    idc  = [base_id]

    for lan in flan_rows:
        if lan["statuslan"] == "C" or not lan["codtdo"]:
            continue
        if random.random() > 0.70:
            continue

        trib_codes = tributos_por_codtdo(lan["codtdo"])
        base_calc  = float(lan["valororiginal"])

        for cod in trib_codes:
            desc, aliq_base = TRIBUTOS[cod]
            aliq  = round(aliq_base * random.uniform(0.80, 1.20), 4)
            valor = round(base_calc * aliq, 2)
            if valor <= 0:
                continue
            rows.append({
                "idftrblan":      nxt(idc),
                "idlan":          lan["idlan"],
                "codcoligada":    lan["codcoligada"],
                "codtributo":     cod,
                "descricao":      desc,
                "aliquota":       aliq,
                "base_calculo":   base_calc,
                "valor":          valor,
                "dt_atualizacao": lan["dt_atualizacao"],
            })

    return rows


# ══════════════════════════════════════════════════════════════════
#  6. Geração de dados dimensionais
# ══════════════════════════════════════════════════════════════════
def gerar_produtos(n: int, ts: datetime) -> list:
    return [{
        "codprod": i, "nomeprod": fake.bs().title()[:100],
        "categoria": random.choice(CATEGORIAS), "unidade": random.choice(UNIDADES),
        "preco_base": round(random.uniform(5, 8000), 2),
        "ativo": random.choices([True, False], weights=[95, 5])[0],
        "dt_atualizacao": ts,
    } for i in range(1, n + 1)]

def gerar_fcfo(ts: datetime) -> list:
    rows = []
    for i in range(1, NUM_FORNEC + 1):
        r = random.random()
        if r < 0.15:
            cnpj = None
        elif r < 0.25:
            cnpj = fake.cnpj().replace(".", "").replace("/", "").replace("-", "")
        else:
            cnpj = fake.cnpj()
        rows.append({
            "codcfo": i,
            "nomecfo": fake.company()[:120],
            "tipocfo": random.choices(["F", "A"], weights=[80, 20])[0],
            "cnpj_cpf": cnpj,
            "email": fake.company_email() if random.random() > 0.2 else None,
            "telefone": fake.phone_number()[:20] if random.random() > 0.3 else None,
            "cidade": fake.city()[:60],
            "uf": fake.estado_sigla(),
            "ativo": random.choices([True, False], weights=[92, 8])[0],
            "dt_atualizacao": ts,
        })
    return rows

def gerar_gccusto(ts: datetime) -> list:
    return [{
        "codccusto": cc,
        "nome": f"Centro {cc} — {random.choice(TIPOS_CCUSTO)}",
        "tipo": random.choice(TIPOS_CCUSTO),
        "responsavel": fake.name()[:80],
        "ativo": True,
        "dt_atualizacao": ts,
    } for cc in CENTROS]

def gerar_tloc(ts: datetime) -> list:
    rows = []
    for i in range(1, NUM_LOCAIS + 1):
        fil = random.choice(FILIAIS)
        tipo = random.choice(TIPOS_LOC)
        rows.append({
            "idloc": i,
            "nome": f"Almox. {tipo} — Filial {fil}",
            "filial": fil,
            "tipo": tipo,
            "ativo": random.choices([True, False], weights=[93, 7])[0],
            "dt_atualizacao": ts,
        })
    return rows

def gerar_fctdo(ts: datetime) -> list:
    return [{
        "codtdo":         r[0],
        "descricao":      r[1],
        "classific":      r[2],
        "observacao":     r[3],
        "dt_atualizacao": ts,
    } for r in FCTDO_DATA]


# ══════════════════════════════════════════════════════════════════
#  7. Geração de flan + flanbaixa (módulo financeiro expandido)
# ══════════════════════════════════════════════════════════════════
def gerar_flan_e_baixas(
    tmov_recs: list,
    ts: datetime,
    base_idlan: int,
    base_idbaixa: int,
) -> tuple:
    """
    Gera flan (schema RM completo) e flanbaixa (baixas parciais + quitação).

    Regras de simulação:
    ─────────────────────────────────────────────────────────────────
    flan
      • Cada REC não-Devolvido gera 1..3 parcelas
      • numerodocumento: '{nf}/{parcela:02d}'  ex: '042891/01'
      • ~5% dos lançamentos são cancelados (statuslan='C')
      • valorop1/2/3 acumulam ISSQN, IRPJ, INSSPJ diretamente na flan
      • valordesconto, valorjuros, valormulta simulados para
        documentos pagos dentro e fora do prazo
      • dataprevbaixa = datavencimento - 0..3 dias

    flanbaixa
      • Lançamentos 'P' têm 1..3 baixas
        – 1 baixa: quitação direta
        – 2+ baixas: parciais aleatórias, última quita o saldo
      • valorbaixado na flan = soma das baixas geradas
      • ~3% dos pagos têm datacancelbaixa
    ─────────────────────────────────────────────────────────────────
    """
    hoje       = datetime.now().date()
    flan_rows  = []
    baixa_rows = []
    idlan      = [base_idlan]
    idbaixa    = [base_idbaixa]

    for rec in tmov_recs:
        if rec["status"] == "Devolvido":
            continue

        codtdo      = random.choices(CODTDO_COMPRA, weights=CODTDO_PESOS)[0]
        n_parcelas  = random.choices([1, 2, 3], weights=[55, 30, 15])[0]
        valor_total = float(rec["valortotal"])
        valor_parc  = round(valor_total / n_parcelas, 2)
        dt_base     = rec["dataemissao"]        # já é date
        filial_cod  = random.choice(FILIAIS_CODIGO)
        nf_num      = num_nf(idlan[0] + 1)

        for parc in range(1, n_parcelas + 1):
            lan_id    = nxt(idlan)
            venc      = dt_base + timedelta(days=30 * parc)
            cancelado = random.random() < 0.05

            # Tributos diretos na flan (valorop1/2/3)
            if codtdo in CODTDO_SERVICO:
                vop1 = round(valor_parc * 0.0500, 2)   # ISSQN
                vop2 = round(valor_parc * 0.0150, 2)   # IRPJ
                vop3 = round(valor_parc * 0.1100, 2)   # INSSPJ
            else:
                vop1, vop2, vop3 = 0.0, 0.0, 0.0

            vencido = venc < hoje
            pago    = not cancelado and vencido and random.random() < 0.75

            if pago:
                dias_atraso = random.randint(0, 45)
                datapag     = venc + timedelta(days=dias_atraso)
                desconto    = round(valor_parc * random.uniform(0, 0.02), 2) if dias_atraso == 0 else 0.0
                juros       = round(valor_parc * 0.001 * dias_atraso, 2) if dias_atraso > 0 else 0.0
                multa       = round(valor_parc * 0.02, 2) if dias_atraso > 3 else 0.0
            else:
                datapag, desconto, juros, multa = None, 0.0, 0.0, 0.0

            # ~3% dos pagos têm a baixa cancelada posteriormente
            cancela_baixa  = pago and random.random() < 0.03
            dt_cancelbaixa = (datapag + timedelta(days=random.randint(1, 10))
                              if cancela_baixa else None)
            status_efetivo_pago = pago and not cancela_baixa

            statuslan = ("C" if cancelado
                         else ("P" if status_efetivo_pago else "A"))

            # ── Baixas (flanbaixa) ──────────────────────────────
            baixas_geradas = []
            if pago and not cancela_baixa:
                n_baixas = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
                if n_baixas == 1:
                    baixas_geradas = [(datapag, valor_parc)]
                else:
                    saldo = valor_parc
                    for b in range(1, n_baixas):
                        frac  = round(saldo * random.uniform(0.20, 0.60), 2)
                        dt_b  = datapag - timedelta(days=(n_baixas - b) * random.randint(1, 7))
                        baixas_geradas.append((dt_b, frac))
                        saldo = round(saldo - frac, 2)
                    baixas_geradas.append((datapag, saldo))

            valorbaixado_total = sum(v for _, v in baixas_geradas)

            flan_rows.append({
                "idlan":             lan_id,
                "codcoligada":       CODCOLIGADA,
                "codfilial":         filial_cod,
                "idmov":             rec["idmov"],
                "codfornec":         rec["codfornec"],
                "codtdo":            codtdo,
                "numerodocumento":   f"{nf_num}/{parc:02d}",
                "numparcela":        parc,
                "totparcelas":       n_parcelas,
                "pagrec":            2,
                "dataemissao":       dt_base,
                "datavencimento":    venc,
                "dataprevbaixa":     venc - timedelta(days=random.randint(0, 3)),
                "datapag":           datapag if status_efetivo_pago else None,
                "datacancelamento":  dt_base + timedelta(days=random.randint(1, 5)) if cancelado else None,
                "datacancelbaixa":   dt_cancelbaixa,
                "valororiginal":     valor_parc,
                "valorop1":          vop1,
                "valorop2":          vop2,
                "valorop3":          vop3,
                "valordesconto":     desconto,
                "valorjuros":        juros,
                "valormulta":        multa,
                "valorbaixado":      valorbaixado_total,
                "statuslan":         statuslan,
                "historico":         f"Parcela {parc}/{n_parcelas} — NF {nf_num} — {rec['numeromov']}",
                "codccusto":         rec.get("codccusto"),
                "dt_atualizacao":    ts,
            })

            for dt_b, val_b in baixas_geradas:
                baixa_rows.append({
                    "idbaixa":        nxt(idbaixa),
                    "idlan":          lan_id,
                    "codcoligada":    CODCOLIGADA,
                    "codfilial":      filial_cod,
                    "databaixa":      dt_b,
                    "valorbaixado":   val_b,
                    "historico":      f"Baixa — idlan {lan_id} — {val_b:.2f}",
                    "dt_atualizacao": ts,
                })

    return flan_rows, baixa_rows


# ══════════════════════════════════════════════════════════════════
#  8. Geração de movimentos (SC → OC → REC) — inalterado
# ══════════════════════════════════════════════════════════════════
def gerar_movimentos(produtos, fcfo_rows, locais, ts, base_mov, base_itm, base_rel, base_irel):
    tmov, titmmov, tmovrelac, titmmovrelac = [], [], [], []
    idx: dict = {}

    mc  = [base_mov]
    ic  = [base_itm]
    rc  = [base_rel]
    irc = [base_irel]

    loc_ids = [l["idloc"] for l in locais if l["ativo"]]

    for _ in range(NUM_SCs):

        # ── SC ───────────────────────────────────────────────────
        sc_dt    = rand_date()
        sc_id    = nxt(mc)
        sc_fil   = random.choice(FILIAIS)
        sc_cc    = random.choice(CENTROS)
        prods_sc = random.sample(produtos, min(random.randint(*ITENS_POR_MOV), len(produtos)))
        sc_user  = random.choice(USUARIOS)

        sc_total  = 0.0
        sc_nitens = []

        for nitem, p in enumerate(prods_sc, start=1):
            qty   = round(random.uniform(1, 200), 3)
            price = round(p["preco_base"] * random.uniform(0.88, 1.12), 2)
            iid   = nxt(ic)
            r = {
                "iditmov": iid, "idmov": sc_id, "nitem": nitem,
                "codprod": p["codprod"], "idloc": None,
                "quantidade": qty, "valorunit": price,
                "valortotal": round(qty * price, 2), "unidade": p["unidade"],
                "observacao": fake.sentence(nb_words=5)[:300] if random.random() < 0.25 else "",
                "dt_atualizacao": ts,
            }
            titmmov.append(r)
            idx[(sc_id, nitem)] = r
            sc_total += r["valortotal"]
            sc_nitens.append(nitem)

        tmov.append({
            "idmov": sc_id, "codtmv": random.choice(CODTMV_SC),
            "numeromov": f"SC{sc_id:08d}",
            "dataemissao": sc_dt.date(), "dataentrega": None,
            "datacompetencia": competencia(sc_dt).date(),
            "codfornec": None, "filial": sc_fil, "codccusto": sc_cc,
            "status": random.choices(STATUS_SC, weights=[10, 20, 60, 10])[0],
            "valortotal": round(sc_total, 2), "desconto": 0.0, "frete": 0.0,
            "observacao": fake.sentence(nb_words=9)[:500] if random.random() < 0.35 else "",
            "usuariocriacao": sc_user, "datacriacao": sc_dt, "dt_atualizacao": ts,
        })

        # ── OCs ──────────────────────────────────────────────────
        for _ in range(random.randint(*OCs_POR_SC)):
            oc_dt   = sc_dt + timedelta(days=random.randint(1, 20))
            oc_id   = nxt(mc)
            forn    = random.choice(fcfo_rows)
            oc_user = random.choice(USUARIOS)

            sub_sc_nitens = random.sample(sc_nitens, k=random.randint(1, len(sc_nitens)))

            tmovrelac.append({
                "idmovrelac": nxt(rc),
                "idmov_orig": sc_id, "idmov_dest": oc_id,
                "tipo_orig": "SC", "tipo_dest": "OC",
                "dt_atualizacao": ts,
            })

            oc_total  = 0.0
            oc_nitens = []

            for nitem in sub_sc_nitens:
                orig  = idx[(sc_id, nitem)]
                qty   = round(orig["quantidade"] * random.uniform(0.5, 1.0), 3)
                price = round(orig["valorunit"]  * random.uniform(0.94, 1.06), 2)
                iid   = nxt(ic)
                r = {
                    "iditmov": iid, "idmov": oc_id, "nitem": nitem,
                    "codprod": orig["codprod"], "idloc": None,
                    "quantidade": qty, "valorunit": price,
                    "valortotal": round(qty * price, 2), "unidade": orig["unidade"],
                    "observacao": "", "dt_atualizacao": ts,
                }
                titmmov.append(r)
                idx[(oc_id, nitem)] = r
                oc_total += r["valortotal"]
                oc_nitens.append(nitem)

                titmmovrelac.append({
                    "iditmmovrelac": nxt(irc),
                    "idmov_orig": sc_id, "nitem_orig": nitem,
                    "idmov_dest": oc_id, "nitem_dest": nitem,
                    "dt_atualizacao": ts,
                })

            desc  = round(oc_total * random.uniform(0, 0.05), 2)
            frete = round(random.uniform(0, 350), 2)
            tmov.append({
                "idmov": oc_id, "codtmv": random.choice(CODTMV_OC),
                "numeromov": f"OC{oc_id:08d}",
                "dataemissao": oc_dt.date(),
                "dataentrega": (oc_dt + timedelta(days=random.randint(7, 45))).date(),
                "datacompetencia": competencia(oc_dt).date(),
                "codfornec": forn["codcfo"], "filial": sc_fil, "codccusto": sc_cc,
                "status": random.choices(STATUS_OC, weights=[15, 20, 55, 10])[0],
                "valortotal": round(oc_total - desc + frete, 2),
                "desconto": desc, "frete": frete, "observacao": "",
                "usuariocriacao": oc_user, "datacriacao": oc_dt, "dt_atualizacao": ts,
            })

            # ── RECs ─────────────────────────────────────────────
            for _ in range(random.randint(*RECs_POR_OC)):
                rec_dt   = oc_dt + timedelta(days=random.randint(5, 60))
                rec_id   = nxt(mc)
                rec_user = random.choice(USUARIOS)

                sub_oc_nitens = random.sample(oc_nitens, k=random.randint(1, len(oc_nitens)))

                tmovrelac.append({
                    "idmovrelac": nxt(rc),
                    "idmov_orig": oc_id, "idmov_dest": rec_id,
                    "tipo_orig": "OC", "tipo_dest": "REC",
                    "dt_atualizacao": ts,
                })

                rec_total = 0.0

                for nitem in sub_oc_nitens:
                    orig2 = idx[(oc_id, nitem)]
                    qty   = round(orig2["quantidade"] * random.uniform(0.3, 1.0), 3)
                    iid   = nxt(ic)
                    r2 = {
                        "iditmov": iid, "idmov": rec_id, "nitem": nitem,
                        "codprod": orig2["codprod"],
                        "idloc": random.choice(loc_ids),
                        "quantidade": qty, "valorunit": orig2["valorunit"],
                        "valortotal": round(qty * orig2["valorunit"], 2),
                        "unidade": orig2["unidade"], "observacao": "",
                        "dt_atualizacao": ts,
                    }
                    titmmov.append(r2)
                    idx[(rec_id, nitem)] = r2
                    rec_total += r2["valortotal"]

                    titmmovrelac.append({
                        "iditmmovrelac": nxt(irc),
                        "idmov_orig": oc_id,  "nitem_orig": nitem,
                        "idmov_dest": rec_id, "nitem_dest": nitem,
                        "dt_atualizacao": ts,
                    })

                status_rec = random.choices(STATUS_REC, weights=[70, 20, 10])[0]
                tmov.append({
                    "idmov": rec_id, "codtmv": random.choice(CODTMV_REC),
                    "numeromov": f"REC{rec_id:08d}",
                    "dataemissao": rec_dt.date(), "dataentrega": rec_dt.date(),
                    "datacompetencia": competencia(rec_dt).date(),
                    "codfornec": forn["codcfo"], "filial": sc_fil, "codccusto": sc_cc,
                    "status": status_rec,
                    "valortotal": round(rec_total, 2), "desconto": 0.0, "frete": 0.0,
                    "observacao": "",
                    "usuariocriacao": rec_user, "datacriacao": rec_dt,
                    "dt_atualizacao": ts,
                })

    return tmov, titmmov, tmovrelac, titmmovrelac


# ══════════════════════════════════════════════════════════════════
#  9. UPDATE — simulação de re-feed
#  Diferenças em relação ao SQL Server:
#    - Placeholders %s em vez de ?
#    - LIMIT n em vez de TOP (n)
#    - RANDOM() em vez de NEWID()
#    - CURRENT_DATE em vez de CAST(GETDATE() AS DATE)
#    - COALESCE em vez de ISNULL
#    - execute_batch em vez de executemany
# ══════════════════════════════════════════════════════════════════
def aplicar_updates(cur, ts: datetime, total_tmov: int):
    print("\n🔄  Aplicando UPDATEs (simulação de re-feed do sistema fonte)...")
    hoje = datetime.now().date()

    # ── tmov ─────────────────────────────────────────────────────
    n = max(1, int(total_tmov * OVERLAP_PERC))
    cur.execute("SELECT idmov, status, valortotal FROM tmov ORDER BY RANDOM() LIMIT %s", (n,))
    rows_tmov = cur.fetchall()
    if rows_tmov:
        upd = [(proximo_status(s), round(float(v or 0) * random.uniform(0.97, 1.03), 2),
                fake.sentence(nb_words=7)[:500] if random.random() < 0.5 else "", ts, i)
               for i, s, v in rows_tmov]
        psycopg2.extras.execute_batch(cur, """
            UPDATE tmov SET status=%s, valortotal=%s, observacao=%s, dt_atualizacao=%s
            WHERE idmov=%s
        """, upd)
        print(f"  ✔ tmov            {len(upd):>6,} atualizados  (status, valortotal, observacao)")

    # ── titmmov ──────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM titmmov")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute("SELECT iditmov, quantidade, valorunit FROM titmmov ORDER BY RANDOM() LIMIT %s", (n,))
    rows_titm = cur.fetchall()
    if rows_titm:
        upd = []
        for iditm, qtd, vunit in rows_titm:
            nq = round(float(qtd or 1) * random.uniform(0.95, 1.05), 3)
            upd.append((nq, round(nq * float(vunit or 0), 2), ts, iditm))
        psycopg2.extras.execute_batch(cur, """
            UPDATE titmmov SET quantidade=%s, valortotal=%s, dt_atualizacao=%s
            WHERE iditmov=%s
        """, upd)
        print(f"  ✔ titmmov         {len(upd):>6,} atualizados  (quantidade, valortotal)")

    # ── pproduto ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM pproduto")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute("SELECT codprod, preco_base, ativo FROM pproduto ORDER BY RANDOM() LIMIT %s", (n,))
    rows_prod = cur.fetchall()
    if rows_prod:
        upd = [(round(float(p or 1) * random.uniform(0.92, 1.08), 2),
                (not a) if random.random() < 0.02 else a, ts, c)
               for c, p, a in rows_prod]
        psycopg2.extras.execute_batch(cur, """
            UPDATE pproduto SET preco_base=%s, ativo=%s, dt_atualizacao=%s
            WHERE codprod=%s
        """, upd)
        print(f"  ✔ pproduto        {len(upd):>6,} atualizados  (preco_base, ativo)")

    # ── tmovrelac ────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM tmovrelac")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    ids = sample_ids(cur, "tmovrelac", "idmovrelac", n)
    if ids:
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE tmovrelac SET dt_atualizacao=%s WHERE idmovrelac=%s",
            [(ts, i) for i in ids]
        )
        print(f"  ✔ tmovrelac       {len(ids):>6,} atualizados  (dt_atualizacao)")

    # ── titmmovrelac ─────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM titmmovrelac")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    ids = sample_ids(cur, "titmmovrelac", "iditmmovrelac", n)
    if ids:
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE titmmovrelac SET dt_atualizacao=%s WHERE iditmmovrelac=%s",
            [(ts, i) for i in ids]
        )
        print(f"  ✔ titmmovrelac    {len(ids):>6,} atualizados  (dt_atualizacao)")

    # ── fcfo ─────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM fcfo")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute("SELECT codcfo, ativo FROM fcfo ORDER BY RANDOM() LIMIT %s", (n,))
    rows_fcfo = cur.fetchall()
    if rows_fcfo:
        upd = [((not a) if random.random() < 0.03 else a, ts, c) for c, a in rows_fcfo]
        psycopg2.extras.execute_batch(
            cur,
            "UPDATE fcfo SET ativo=%s, dt_atualizacao=%s WHERE codcfo=%s",
            upd
        )
        print(f"  ✔ fcfo            {len(upd):>6,} atualizados  (ativo)")

    # ── flan — simula pagamentos de parcelas vencidas em aberto ──
    # CURRENT_DATE substitui CAST(GETDATE() AS DATE) do SQL Server
    cur.execute("""
        SELECT idlan, valororiginal, valorbaixado, codfilial, codcoligada
        FROM flan
        WHERE statuslan = 'A'
          AND datavencimento < CURRENT_DATE
          AND datacancelamento IS NULL
        ORDER BY RANDOM()
        LIMIT 500
    """)
    rows_flan = cur.fetchall()
    flan_upd  = []
    baixa_upd = []

    cur.execute("SELECT COALESCE(MAX(idbaixa), 0) FROM flanbaixa")
    max_baixa = [cur.fetchone()[0]]

    for idlan, voriginal, vbaixado, codfilial, codcol in rows_flan:
        if random.random() > 0.40:
            continue
        dt_pag = hoje + timedelta(days=random.randint(0, 5))
        saldo  = round(float(voriginal or 0) - float(vbaixado or 0), 2)
        if saldo <= 0:
            continue
        juros = round(saldo * 0.001 * random.randint(0, 30), 2)
        multa = round(saldo * 0.02, 2) if random.random() < 0.4 else 0.0
        flan_upd.append((dt_pag, juros, multa, round(float(vbaixado or 0) + saldo, 2), ts, idlan))
        baixa_upd.append({
            "idbaixa":        nxt(max_baixa),
            "idlan":          idlan,
            "codcoligada":    codcol,
            "codfilial":      codfilial,
            "databaixa":      dt_pag,
            "valorbaixado":   saldo,
            "historico":      f"Quitação via update — idlan {idlan}",
            "dt_atualizacao": ts,
        })

    if flan_upd:
        psycopg2.extras.execute_batch(cur, """
            UPDATE flan
            SET datapag=%s, valorjuros=%s, valormulta=%s,
                valorbaixado=%s, statuslan='P', dt_atualizacao=%s
            WHERE idlan=%s
        """, flan_upd)
        print(f"  ✔ flan            {len(flan_upd):>6,} atualizados  (statuslan→'P', datapag, juros/multa)")

    if baixa_upd:
        insert_batch(cur, "flanbaixa",
            ["idbaixa","idlan","codcoligada","codfilial","databaixa","valorbaixado",
             "historico","dt_atualizacao"],
            baixa_upd)

    # ── ftrblan — tributos de pagamentos novos sem registro ainda ─
    cur.execute("""
        SELECT f.idlan, f.codcoligada, f.valororiginal, f.codtdo
        FROM flan f
        WHERE f.statuslan = 'P'
          AND NOT EXISTS (SELECT 1 FROM ftrblan t WHERE t.idlan = f.idlan)
        ORDER BY RANDOM()
        LIMIT 200
    """)
    rows_trib = cur.fetchall()
    if rows_trib:
        cur.execute("SELECT COALESCE(MAX(idftrblan), 0) FROM ftrblan")
        max_trib = [cur.fetchone()[0]]
        trib_rows = []
        for idlan, codcol, voriginal, codtdo in rows_trib:
            if not codtdo:
                continue
            for cod in tributos_por_codtdo(codtdo):
                desc, aliq_base = TRIBUTOS[cod]
                aliq  = round(aliq_base * random.uniform(0.80, 1.20), 4)
                valor = round(float(voriginal or 0) * aliq, 2)
                if valor <= 0:
                    continue
                trib_rows.append({
                    "idftrblan":      nxt(max_trib),
                    "idlan":          idlan,
                    "codcoligada":    codcol,
                    "codtributo":     cod,
                    "descricao":      desc,
                    "aliquota":       aliq,
                    "base_calculo":   float(voriginal or 0),
                    "valor":          valor,
                    "dt_atualizacao": ts,
                })
        if trib_rows:
            insert_batch(cur, "ftrblan",
                ["idftrblan","idlan","codcoligada","codtributo","descricao",
                 "aliquota","base_calculo","valor","dt_atualizacao"],
                trib_rows)
            print(f"  ✔ ftrblan         {len(trib_rows):>6,} inseridos    (tributos de pagamentos novos)")


# ══════════════════════════════════════════════════════════════════
#  10. Infraestrutura PostgreSQL
# ══════════════════════════════════════════════════════════════════
def conectar(db: str = None) -> psycopg2.extensions.connection:
    """Retorna uma conexão psycopg2 com autocommit habilitado."""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=db or PG_DATABASE,
        user=PG_USER,
        password=PG_PWD,
    )
    conn.autocommit = True
    return conn

def criar_tabelas(conn):
    cur = conn.cursor()

    ordem_drop   = ["ftrblan","flanbaixa","flan","titmmovrelac","titmmov",
                    "tmovrelac","tmov","pproduto","tloc","gccusto","fcfo","fctdo","ttmv"]
    ordem_create = ["ttmv","fctdo","fcfo","gccusto","tloc","pproduto",
                    "tmov","titmmov","tmovrelac","titmmovrelac",
                    "flan","flanbaixa","ftrblan"]

    if DROP_RECREATE:
        print("  ⚙  DROP_RECREATE=True — removendo tabelas...")
        for t in ordem_drop:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")

    print("  ⚙  Criando tabelas (IF NOT EXISTS)...")
    for t in ordem_create:
        cur.execute(DDL_TABLES[t])
    print("  ✔ Tabelas prontas.")

def ler_max_ids(cur) -> dict:
    def mx(tabela, col):
        cur.execute(f"SELECT COALESCE(MAX({col}), 0) FROM {tabela}")
        return cur.fetchone()[0]
    return {
        "mov":     mx("tmov",         "idmov"),
        "itm":     mx("titmmov",      "iditmov"),
        "rel":     mx("tmovrelac",    "idmovrelac"),
        "irel":    mx("titmmovrelac", "iditmmovrelac"),
        "lan":     mx("flan",         "idlan"),
        "baixa":   mx("flanbaixa",    "idbaixa"),
        "tributo": mx("ftrblan",      "idftrblan"),
    }

def insert_batch(cur, tabela: str, colunas: list, dados: list):
    """
    Insere registros em lote usando psycopg2.extras.execute_batch.
    Mais eficiente que executemany para grandes volumes no PostgreSQL.
    """
    if not dados:
        return
    ph  = ",".join(["%s"] * len(colunas))
    sql = f"INSERT INTO {tabela} ({','.join(colunas)}) VALUES ({ph})"
    rows = [[r[c] for c in colunas] for r in dados]
    for i in range(0, len(rows), BATCH_SIZE):
        psycopg2.extras.execute_batch(cur, sql, rows[i:i+BATCH_SIZE])
    print(f"  ✔ {tabela:<22} {len(rows):>7,} registros inseridos")


# ══════════════════════════════════════════════════════════════════
#  11. MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    sep = "═" * 64
    ts  = datetime.now()

    print(f"\n{sep}")
    print(f"  OLTP Suprimentos + Financeiro — {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    print(sep)

    print(f"\n🔌  Conectando em [{PG_HOST}:{PG_PORT}/{PG_DATABASE}]...")
    try:
        conn = conectar()
    except Exception as e:
        print(f"\n❌  Erro de conexão: {e}")
        print("     Verifique PG_HOST, PG_PORT, PG_DATABASE, PG_USER e PG_PASSWORD no .env")
        return

    criar_tabelas(conn)
    cur = conn.cursor()

    ids      = ler_max_ids(cur)
    primeira = (ids["mov"] == 0)

    print(f"\n  📌 Estado atual do banco:")
    print(f"     MAX idmov          = {ids['mov']:,}")
    print(f"     MAX iditmov        = {ids['itm']:,}")
    print(f"     MAX idmovrelac     = {ids['rel']:,}")
    print(f"     MAX iditmmovrelac  = {ids['irel']:,}")
    print(f"     MAX idlan          = {ids['lan']:,}")
    print(f"     MAX idbaixa        = {ids['baixa']:,}")
    print(f"     MAX idftrblan      = {ids['tributo']:,}")
    print(f"     Modo               = {'PRIMEIRA CARGA' if primeira else 'INCREMENTAL + UPDATE'}")

    print(f"\n🔧  Gerando {NUM_SCs} novas SCs e cadeia OC → REC...")

    if primeira:
        produtos  = gerar_produtos(NUM_PRODUTOS, ts)
        fcfo_rows = gerar_fcfo(ts)
        gccusto   = gerar_gccusto(ts)
        locais    = gerar_tloc(ts)
        fctdo     = gerar_fctdo(ts)
    else:
        cur.execute("SELECT codprod, preco_base, unidade FROM pproduto")
        produtos = [{"codprod": r[0], "preco_base": float(r[1] or 100), "unidade": r[2] or "UN"}
                    for r in cur.fetchall()]
        cur.execute("SELECT codcfo FROM fcfo")
        fcfo_rows = [{"codcfo": r[0]} for r in cur.fetchall()]
        cur.execute("SELECT idloc FROM tloc")
        locais = [{"idloc": r[0], "ativo": True} for r in cur.fetchall()]
        gccusto = []
        fctdo   = []

    if not produtos:
        print("\n❌  Nenhum produto encontrado. Verifique a tabela pproduto.")
        conn.close()
        return
    if not fcfo_rows:
        print("\n❌  Nenhum fornecedor encontrado. Verifique a tabela fcfo.")
        conn.close()
        return
    if not locais:
        print("\n❌  Nenhum local de estoque encontrado. Verifique a tabela tloc.")
        conn.close()
        return

    tmov, titmmov, tmovrelac, titmmovrelac = gerar_movimentos(
        produtos, fcfo_rows, locais, ts,
        ids["mov"], ids["itm"], ids["rel"], ids["irel"]
    )

    tmov_recs = [m for m in tmov if m["codtmv"] in CODTMV_REC]

    flan_rows, baixa_rows = gerar_flan_e_baixas(
        tmov_recs, ts, ids["lan"], ids["baixa"]
    )

    ftrblan_rows = gerar_ftrblan(flan_rows, ids["tributo"])

    sc_ct   = sum(1 for m in tmov if m["codtmv"] in CODTMV_SC)
    oc_ct   = sum(1 for m in tmov if m["codtmv"] in CODTMV_OC)
    rec_ct  = len(tmov_recs)
    pago_ct = sum(1 for f in flan_rows if f["statuslan"] == "P")
    canc_ct = sum(1 for f in flan_rows if f["statuslan"] == "C")

    print(f"\n  📊 Novos registros gerados:")
    print(f"     tmov        → SC:{sc_ct:,}  OC:{oc_ct:,}  REC:{rec_ct:,}  Total:{len(tmov):,}")
    print(f"     titmmov     → {len(titmmov):,}")
    print(f"     tmovrelac   → {len(tmovrelac):,}")
    print(f"     titmmovrelac→ {len(titmmovrelac):,}")
    print(f"     flan        → {len(flan_rows):,}  (Pago:{pago_ct:,}  Cancelado:{canc_ct:,}  Aberto:{len(flan_rows)-pago_ct-canc_ct:,})")
    print(f"     flanbaixa   → {len(baixa_rows):,}  (baixas parciais + quitações)")
    print(f"     ftrblan     → {len(ftrblan_rows):,}  (ISSQN/IRPJ/INSSPJ/PIS/COFINS/CSLL/IOF)")

    print(f"\n📥  Inserindo novos registros...")

    if primeira:
        insert_batch(cur, "ttmv",
            ["codtmv","nome","tipo","descricao","dt_atualizacao"],
            [{"codtmv":r[0],"nome":r[1],"tipo":r[2],"descricao":r[3],"dt_atualizacao":ts}
             for r in TTMV_DATA])
        insert_batch(cur, "fctdo",
            ["codtdo","descricao","classific","observacao","dt_atualizacao"],
            fctdo)
        insert_batch(cur, "fcfo",
            ["codcfo","nomecfo","tipocfo","cnpj_cpf","email","telefone",
             "cidade","uf","ativo","dt_atualizacao"],
            fcfo_rows)
        insert_batch(cur, "gccusto",
            ["codccusto","nome","tipo","responsavel","ativo","dt_atualizacao"],
            gccusto)
        insert_batch(cur, "tloc",
            ["idloc","nome","filial","tipo","ativo","dt_atualizacao"],
            locais)
        insert_batch(cur, "pproduto",
            ["codprod","nomeprod","categoria","unidade","preco_base","ativo","dt_atualizacao"],
            produtos)

    insert_batch(cur, "tmov",
        ["idmov","codtmv","numeromov","dataemissao","dataentrega","datacompetencia",
         "codfornec","filial","codccusto","status",
         "valortotal","desconto","frete","observacao",
         "usuariocriacao","datacriacao","dt_atualizacao"],
        tmov)

    insert_batch(cur, "titmmov",
        ["iditmov","idmov","nitem","codprod","idloc","quantidade","valorunit",
         "valortotal","unidade","observacao","dt_atualizacao"],
        titmmov)

    insert_batch(cur, "tmovrelac",
        ["idmovrelac","idmov_orig","idmov_dest","tipo_orig","tipo_dest","dt_atualizacao"],
        tmovrelac)

    insert_batch(cur, "titmmovrelac",
        ["iditmmovrelac","idmov_orig","nitem_orig","idmov_dest","nitem_dest","dt_atualizacao"],
        titmmovrelac)

    insert_batch(cur, "flan",
        ["idlan","codcoligada","codfilial","idmov","codfornec","codtdo",
         "numerodocumento","numparcela","totparcelas","pagrec",
         "dataemissao","datavencimento","dataprevbaixa","datapag",
         "datacancelamento","datacancelbaixa",
         "valororiginal","valorop1","valorop2","valorop3",
         "valordesconto","valorjuros","valormulta","valorbaixado",
         "statuslan","historico","codccusto","dt_atualizacao"],
        flan_rows)

    insert_batch(cur, "flanbaixa",
        ["idbaixa","idlan","codcoligada","codfilial","databaixa","valorbaixado",
         "historico","dt_atualizacao"],
        baixa_rows)

    insert_batch(cur, "ftrblan",
        ["idftrblan","idlan","codcoligada","codtributo","descricao",
         "aliquota","base_calculo","valor","dt_atualizacao"],
        ftrblan_rows)

    if not primeira:
        aplicar_updates(cur, ts, ids["mov"])
    else:
        print("\n  ℹ️  Primeira execução — UPDATEs pulados.")
        print("     Mude DROP_RECREATE=False e rode novamente para simular o re-feed.")

    conn.close()

    conn2 = conectar()
    cur2  = conn2.cursor()
    print(f"\n{'─'*64}")
    print(f"  📦 Totais acumulados no banco:")
    for t in ["tmov","titmmov","tmovrelac","titmmovrelac",
              "pproduto","fcfo","gccusto","tloc","fctdo",
              "flan","flanbaixa","ftrblan"]:
        cur2.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"     {t:<24} {cur2.fetchone()[0]:>8,} registros")
    conn2.close()
    print(f"\n✅  Concluído! [{ts.strftime('%H:%M:%S')}]\n")


if __name__ == "__main__":
    main()