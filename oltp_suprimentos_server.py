"""
╔══════════════════════════════════════════════════════════════════╗
║   Gerador de Base OLTP — Suprimentos + Financeiro                ║
║   (Simulação RM/TOTVS)                                           ║
║   Destino : SQL Server (localhost)                               ║
║   Modo    : INCREMENTAL + UPDATE de registros existentes         ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Cadastros / Dimensões:                                ║
║    TTMV          → Tipos de Movimento                            ║
║    FCFO          → Cadastro de Clientes/Fornecedores             ║
║    GCCUSTO       → Centro de Custo                               ║
║    TLOC          → Locais de Estoque (Almoxarifados)             ║
║    PPRODUTO      → Produtos                                      ║
║    FCTDO         → Tipos de Documento Financeiro (novo)          ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Movimentos / Transações:                              ║
║    TMOV          → Cabeçalho dos Movimentos (SC / OC / REC)      ║
║    TITMMOV       → Itens dos Movimentos                          ║
║    TMOVRELAC     → Ponte entre cabeçalhos (SC→OC, OC→REC)        ║
║    TITMMOVRELAC  → Ponte entre itens                             ║
╠══════════════════════════════════════════════════════════════════╣
║  Tabelas — Financeiro (módulo expandido):                        ║
║    FLAN          → Lançamentos Financeiros (schema real RM)      ║
║    FLANBAIXA     → Baixas parciais e quitação por lançamento     ║
║    FTRBLAN       → Tributos por lançamento (ISSQN/IRPJ/INSS/     ║
║                    PIS/COFINS/CSLL/IOF)                          ║
╠══════════════════════════════════════════════════════════════════╣
║  MÓDULO FINANCEIRO — detalhes de simulação:                      ║
║                                                                  ║
║  FLAN                                                            ║
║    • Schema alinhado ao RM real: CODCOLIGADA, CODFILIAL,         ║
║      CODTDO, NUMERODOCUMENTO (NF/parcela ex: "000123/01"),       ║
║      DATAPAG, DATAPREVBAIXA, DATACANCELAMENTO, DATACANCELBAIXA,  ║
║      VALOROP1 (ISSQN), VALOROP2 (IRPJ), VALOROP3 (INSSPJ),      ║
║      VALORDESCONTO, VALORJUROS, VALORMULTA, VALORBAIXADO         ║
║    • ~5% dos lançamentos são cancelados (DATACANCELAMENTO)       ║
║    • ~3% têm baixa cancelada após pagamento (DATACANCELBAIXA)    ║
║                                                                  ║
║  FLANBAIXA                                                       ║
║    • Lançamentos pagos podem ter 1..3 baixas parciais antes      ║
║      da quitação final                                           ║
║    • Cada baixa tem DATABAIXA, VALORBAIXADO, CODFILIAL,          ║
║      CODCOLIGADA e HISTORICO                                     ║
║    • Quitação = soma das baixas parciais ≥ VALORORIGINAL         ║
║                                                                  ║
║  FTRBLAN                                                         ║
║    • Tributos calculados por lançamento                          ║
║    • Incidência varia por CODTDO (NF Serviço tributa mais)       ║
║    • Alíquotas simuladas: ISSQN 5%, IRPJ 1.5%, INSSPJ 11%,       ║
║      PIS 0.65%, COFINS 3%, CSLL 1%, IOF (só financeiro)          ║
║    • Uma linha por tributo por lançamento (CODTRIBUTO)           ║
║                                                                  ║
║  FCTDO                                                           ║
║    • Tipos de documento: NF Mercadoria, NF Serviço, Boleto,      ║
║      Duplicata, Recibo, Adiantamento, etc.                       ║
║    • CODTDO '23' excluído das análises (estorno), como na query  ║
╠══════════════════════════════════════════════════════════════════╣
║  ESTRUTURA DE ITENS — padrão RM:                                 ║
║    TITMMOV: NITEM reinicia a cada movimento. PK real: (IDMOV, NITEM) ║
║    IDITMOV: surrogate key apenas para facilitar JOINs            ║
║    TITMMOVRELAC: rastreabilidade por IDMOV_ORIG + NITEM_ORIG     ║
╠══════════════════════════════════════════════════════════════════╣
║  Comportamento a cada execução:                                  ║
║    1ª (DROP_RECREATE=True)  → cria e popula do zero              ║
║    2ª+ (DROP_RECREATE=False)→ insere novos + UPDATE 10% antigos  ║
╚══════════════════════════════════════════════════════════════════╝

Dependências:
    pip install pyodbc faker python-dotenv

Driver ODBC:
    - "ODBC Driver 18 for SQL Server"  ← recomendado
    - "ODBC Driver 17 for SQL Server"
    Download: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server
"""

import os
import pyodbc
import random
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timedelta

# ══════════════════════════════════════════════════════════════════
#  ▶  CONFIG
# ══════════════════════════════════════════════════════════════════

load_dotenv()

SQL_TRUSTED  = True
SQL_SERVER   = os.getenv('SQLSERVER_HOST')
SQL_DATABASE = os.getenv('SQLSERVER_DBNAME')
SQL_USER     = os.getenv('SQLSERVER_USER')
SQL_PWD      = os.getenv('SQLSERVER_PASSWORD')
SQL_DRIVER   = "ODBC Driver 17 for SQL Server"

NUM_SCs        = 200
OCs_POR_SC     = (1, 3)
RECs_POR_OC    = (1, 3)
ITENS_POR_MOV  = (1, 5)
NUM_PRODUTOS   = 200
NUM_FORNEC     = 60
NUM_LOCAIS     = 8      # almoxarifados / locais de estoque

OVERLAP_PERC   = 0.10
DROP_RECREATE  = True
BATCH_SIZE     = 2000
SEED           = None

# Coligada e filiais simuladas (padrão RM multi-empresa)
CODCOLIGADA    = 1
FILIAIS_CODIGO = [1, 2, 3, 4]   # int — CODFILIAL na FLAN/FLANBAIXA

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
# CODTDO '23' = estorno — excluído nas análises financeiras (WHERE CODTDO <> '23')
# Mantém o mesmo comportamento da query de tesouraria.
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

# CODTDO associados a serviços (tributam ISSQN e INSSPJ)
CODTDO_SERVICO = {"02", "05", "07"}
# CODTDO onde IOF pode incidir
CODTDO_FINANCEIRO = {"06"}

# Pesos de sorteio para CODTDO nos lançamentos de compra (exclui estorno e devolução)
CODTDO_COMPRA = ["01", "02", "03", "04", "05", "06", "07", "09", "10"]
CODTDO_PESOS  = [30,   15,   20,   15,    5,    5,    5,    3,    2  ]

# ══════════════════════════════════════════════════════════════════
#  3. DDL
# ══════════════════════════════════════════════════════════════════
DDL_TABLES = {

"TTMV": """CREATE TABLE TTMV (
    CODTMV         VARCHAR(10)  NOT NULL,
    NOME           VARCHAR(60)  NOT NULL,
    TIPO           CHAR(3)      NOT NULL CHECK (TIPO IN ('SC','OC','REC')),
    DESCRICAO      VARCHAR(200) NULL,
    DT_ATUALIZACAO DATETIME2    NOT NULL,
    CONSTRAINT PK_TTMV PRIMARY KEY (CODTMV)
)""",

"FCTDO": """CREATE TABLE FCTDO (
    CODTDO         VARCHAR(5)   NOT NULL,
    DESCRICAO      VARCHAR(80)  NOT NULL,
    CLASSIFIC      CHAR(1)      NOT NULL CHECK (CLASSIFIC IN ('C','D','E')),  -- Crédito/Débito/Estorno
    OBSERVACAO     VARCHAR(200) NULL,
    DT_ATUALIZACAO DATETIME2    NOT NULL,
    CONSTRAINT PK_FCTDO PRIMARY KEY (CODTDO)
)""",

# ── FCFO ──────────────────────────────────────────────────────────
"FCFO": """CREATE TABLE FCFO (
    CODCFO         INT           NOT NULL,
    NOMECFO        VARCHAR(120)  NOT NULL,
    TIPOCFO        CHAR(1)       NOT NULL CHECK (TIPOCFO IN ('C','F','A')),
    CNPJ_CPF       VARCHAR(20)   NULL,
    EMAIL          VARCHAR(100)  NULL,
    TELEFONE       VARCHAR(20)   NULL,
    CIDADE         VARCHAR(60)   NULL,
    UF             CHAR(2)       NULL,
    ATIVO          BIT           NOT NULL DEFAULT 1,
    DT_ATUALIZACAO DATETIME2     NOT NULL,
    CONSTRAINT PK_FCFO PRIMARY KEY (CODCFO)
)""",

"GCCUSTO": """CREATE TABLE GCCUSTO (
    CODCCUSTO      VARCHAR(20)  NOT NULL,
    NOME           VARCHAR(80)  NOT NULL,
    TIPO           VARCHAR(20)  NULL,
    RESPONSAVEL    VARCHAR(80)  NULL,
    ATIVO          BIT          NOT NULL DEFAULT 1,
    DT_ATUALIZACAO DATETIME2    NOT NULL,
    CONSTRAINT PK_GCCUSTO PRIMARY KEY (CODCCUSTO)
)""",

"TLOC": """CREATE TABLE TLOC (
    IDLOC          INT          NOT NULL,
    NOME           VARCHAR(80)  NOT NULL,
    FILIAL         VARCHAR(10)  NULL,
    TIPO           VARCHAR(20)  NULL,
    ATIVO          BIT          NOT NULL DEFAULT 1,
    DT_ATUALIZACAO DATETIME2    NOT NULL,
    CONSTRAINT PK_TLOC PRIMARY KEY (IDLOC)
)""",

"PPRODUTO": """CREATE TABLE PPRODUTO (
    CODPROD        INT           NOT NULL,
    NOMEPROD       VARCHAR(100)  NOT NULL,
    CATEGORIA      VARCHAR(40)   NULL,
    UNIDADE        VARCHAR(6)    NULL,
    PRECO_BASE     DECIMAL(15,2) NULL,
    ATIVO          BIT           NOT NULL DEFAULT 1,
    DT_ATUALIZACAO DATETIME2     NOT NULL,
    CONSTRAINT PK_PPRODUTO PRIMARY KEY (CODPROD)
)""",

"TMOV": """CREATE TABLE TMOV (
    IDMOV            INT           NOT NULL,
    CODTMV           VARCHAR(10)   NOT NULL,
    NUMEROMOV        VARCHAR(20)   NOT NULL,
    DATAEMISSAO      DATE          NULL,
    DATAENTREGA      DATE          NULL,
    DATACOMPETENCIA  DATE          NULL,
    CODFORNEC        INT           NULL,
    FILIAL           VARCHAR(10)   NULL,
    CODCCUSTO        VARCHAR(20)   NULL,
    STATUS           VARCHAR(20)   NULL,
    VALORTOTAL       DECIMAL(15,2) NULL DEFAULT 0,
    DESCONTO         DECIMAL(15,2) NULL DEFAULT 0,
    FRETE            DECIMAL(15,2) NULL DEFAULT 0,
    OBSERVACAO       VARCHAR(500)  NULL,
    USUARIOCRIACAO   VARCHAR(40)   NULL,
    DATACRIACAO      DATETIME2     NULL,
    DT_ATUALIZACAO   DATETIME2     NOT NULL,
    CONSTRAINT PK_TMOV      PRIMARY KEY (IDMOV),
    CONSTRAINT UQ_NUMEROMOV UNIQUE (NUMEROMOV),
    CONSTRAINT FK_TMOV_TTMV FOREIGN KEY (CODTMV)    REFERENCES TTMV(CODTMV),
    CONSTRAINT FK_TMOV_FCFO FOREIGN KEY (CODFORNEC)  REFERENCES FCFO(CODCFO)
)""",

"TITMMOV": """CREATE TABLE TITMMOV (
    IDITMOV        INT           NOT NULL,
    IDMOV          INT           NOT NULL,
    NITEM          SMALLINT      NOT NULL,
    CODPROD        INT           NOT NULL,
    IDLOC          INT           NULL,
    QUANTIDADE     DECIMAL(15,3) NULL,
    VALORUNIT      DECIMAL(15,2) NULL,
    VALORTOTAL     DECIMAL(15,2) NULL,
    UNIDADE        VARCHAR(6)    NULL,
    OBSERVACAO     VARCHAR(300)  NULL,
    DT_ATUALIZACAO DATETIME2     NOT NULL,
    CONSTRAINT PK_TITMMOV       PRIMARY KEY (IDITMOV),
    CONSTRAINT UQ_TITMMOV_NITEM UNIQUE (IDMOV, NITEM),
    CONSTRAINT FK_TITMMOV_TMOV  FOREIGN KEY (IDMOV)   REFERENCES TMOV(IDMOV),
    CONSTRAINT FK_TITMMOV_PROD  FOREIGN KEY (CODPROD) REFERENCES PPRODUTO(CODPROD),
    CONSTRAINT FK_TITMMOV_LOC   FOREIGN KEY (IDLOC)   REFERENCES TLOC(IDLOC)
)""",

"TMOVRELAC": """CREATE TABLE TMOVRELAC (
    IDMOVRELAC     INT       NOT NULL,
    IDMOV_ORIG     INT       NOT NULL,
    IDMOV_DEST     INT       NOT NULL,
    TIPO_ORIG      CHAR(3)   NOT NULL,
    TIPO_DEST      CHAR(3)   NOT NULL,
    DT_ATUALIZACAO DATETIME2 NOT NULL,
    CONSTRAINT PK_TMOVRELAC      PRIMARY KEY (IDMOVRELAC),
    CONSTRAINT FK_TMOVRELAC_ORIG FOREIGN KEY (IDMOV_ORIG) REFERENCES TMOV(IDMOV),
    CONSTRAINT FK_TMOVRELAC_DEST FOREIGN KEY (IDMOV_DEST) REFERENCES TMOV(IDMOV)
)""",

"TITMMOVRELAC": """CREATE TABLE TITMMOVRELAC (
    IDITMMOVRELAC  INT       NOT NULL,
    IDMOV_ORIG     INT       NOT NULL,
    NITEM_ORIG     SMALLINT  NOT NULL,
    IDMOV_DEST     INT       NOT NULL,
    NITEM_DEST     SMALLINT  NOT NULL,
    DT_ATUALIZACAO DATETIME2 NOT NULL,
    CONSTRAINT PK_TITMMOVRELAC  PRIMARY KEY (IDITMMOVRELAC),
    CONSTRAINT FK_ITMRELAC_ORIG FOREIGN KEY (IDMOV_ORIG, NITEM_ORIG)
                                REFERENCES TITMMOV (IDMOV, NITEM),
    CONSTRAINT FK_ITMRELAC_DEST FOREIGN KEY (IDMOV_DEST, NITEM_DEST)
                                REFERENCES TITMMOV (IDMOV, NITEM)
)""",

# ── FLAN — schema expandido (alinhado ao RM real) ─────────────────
#
# Principais adições vs. versão anterior:
#   CODCOLIGADA    : empresa (multi-tenant, padrão RM)
#   CODFILIAL      : filial de origem do lançamento
#   CODTDO         : tipo de documento (FK para FCTDO)
#   NUMERODOCUMENTO: número da NF/boleto + parcela ex: "000123/01"
#                    formato permite extrair NOTA e NUMERO_PARCELA
#                    igual à lógica da query de tesouraria
#   DATAEMISSAO    : emissão do documento (≠ DATACRIACAO)
#   DATAPREVBAIXA  : previsão de baixa (agenda de tesouraria)
#   DATAPAG        : data do pagamento efetivo
#   DATACANCELAMENTO   : cancelamento do lançamento
#   DATACANCELBAIXA    : cancelamento de uma baixa já realizada
#   VALOROP1 (ISSQN)   : dedução ISS sobre serviço
#   VALOROP2 (IRPJ)    : dedução IR na fonte
#   VALOROP3 (INSSPJ)  : dedução INSS PJ
#   VALORDESCONTO      : desconto concedido/obtido
#   VALORJUROS         : juros de mora cobrados/pagos
#   VALORMULTA         : multa por atraso
#   VALORBAIXADO       : soma acumulada de baixas (atualizado pela FLANBAIXA)
#
# Problema clássico preservado:
#   JOIN TMOV → FLAN multiplica VALORTOTAL do REC pelo n° de parcelas.
#   Aggregar pela FLAN (SUM VALORORIGINAL) dá o total financeiro correto.
"FLAN": """CREATE TABLE FLAN (
    IDLAN              INT           NOT NULL,
    CODCOLIGADA        TINYINT       NOT NULL DEFAULT 1,
    CODFILIAL          TINYINT       NOT NULL DEFAULT 1,
    IDMOV              INT           NULL,
    CODFORNEC          INT           NULL,
    CODTDO             VARCHAR(5)    NULL,
    NUMERODOCUMENTO    VARCHAR(30)   NULL,   -- ex: '000123/01'  (NF/parcela)
    NUMPARCELA         TINYINT       NOT NULL DEFAULT 1,
    TOTPARCELAS        TINYINT       NOT NULL DEFAULT 1,
    PAGREC             TINYINT       NOT NULL DEFAULT 2,  -- 1=Receber 2=Pagar
    DATAEMISSAO        DATE          NULL,
    DATAVENCIMENTO     DATE          NOT NULL,
    DATAPREVBAIXA      DATE          NULL,
    DATAPAG            DATE          NULL,   -- data do pagamento efetivo
    DATACANCELAMENTO   DATE          NULL,
    DATACANCELBAIXA    DATE          NULL,
    VALORORIGINAL      DECIMAL(15,2) NOT NULL,
    VALOROP1           DECIMAL(15,2) NOT NULL DEFAULT 0,  -- ISSQN
    VALOROP2           DECIMAL(15,2) NOT NULL DEFAULT 0,  -- IRPJ
    VALOROP3           DECIMAL(15,2) NOT NULL DEFAULT 0,  -- INSSPJ
    VALORDESCONTO      DECIMAL(15,2) NOT NULL DEFAULT 0,
    VALORJUROS         DECIMAL(15,2) NOT NULL DEFAULT 0,
    VALORMULTA         DECIMAL(15,2) NOT NULL DEFAULT 0,
    VALORBAIXADO       DECIMAL(15,2) NOT NULL DEFAULT 0,  -- atualizado pelas baixas
    STATUSLAN          CHAR(1)       NOT NULL DEFAULT 'A', -- A=Aberto P=Pago C=Cancelado
    HISTORICO          VARCHAR(200)  NULL,
    CODCCUSTO          VARCHAR(20)   NULL,
    DT_ATUALIZACAO     DATETIME2     NOT NULL,
    CONSTRAINT PK_FLAN       PRIMARY KEY (IDLAN),
    CONSTRAINT FK_FLAN_TMOV  FOREIGN KEY (IDMOV)      REFERENCES TMOV(IDMOV),
    CONSTRAINT FK_FLAN_FCFO  FOREIGN KEY (CODFORNEC)   REFERENCES FCFO(CODCFO),
    CONSTRAINT FK_FLAN_FCTDO FOREIGN KEY (CODTDO)      REFERENCES FCTDO(CODTDO)
)""",

# ── FLANBAIXA ─────────────────────────────────────────────────────
#
# Registra cada evento de baixa (parcial ou total) de um lançamento.
# Um lançamento pode ter 1..N baixas antes da quitação.
#
# Fluxo típico simulado:
#   Lançamento A: Aberto (STATUSLAN='A', VALORBAIXADO=0)
#     → Baixa 1: R$ 300 (parcial)   VALORBAIXADO passa a 300
#     → Baixa 2: R$ 200 (quitação)  VALORBAIXADO passa a 500, FLAN.STATUSLAN='P'
#
# DATABAIXA  : data em que a baixa ocorreu
# VALORBAIXADO: valor desta baixa específica (não acumulado)
# HISTORICO  : campo livre (ex: "Baixa parcial — transf. 12345")
"FLANBAIXA": """CREATE TABLE FLANBAIXA (
    IDBAIXA        INT           NOT NULL,
    IDLAN          INT           NOT NULL,
    CODCOLIGADA    TINYINT       NOT NULL DEFAULT 1,
    CODFILIAL      TINYINT       NOT NULL DEFAULT 1,
    DATABAIXA      DATE          NOT NULL,
    VALORBAIXADO   DECIMAL(15,2) NOT NULL,
    HISTORICO      VARCHAR(200)  NULL,
    DT_ATUALIZACAO DATETIME2     NOT NULL,
    CONSTRAINT PK_FLANBAIXA      PRIMARY KEY (IDBAIXA),
    CONSTRAINT FK_FLANBAIXA_FLAN FOREIGN KEY (IDLAN) REFERENCES FLAN(IDLAN)
)""",

# ── FTRBLAN ───────────────────────────────────────────────────────
#
# Tributos retidos / calculados por lançamento financeiro.
# Cada linha = um tributo específico para um IDLAN.
#
# CODTRIBUTO  : código do tributo (ver TRIBUTOS dict abaixo)
# VALOR       : valor calculado com base na alíquota × base
# ALIQUOTA    : alíquota aplicada (0..1)
# BASE_CALCULO: base sobre a qual o tributo foi calculado
#
# Incidência por tipo de documento (CODTDO):
#   Serviço (02,05,07)  → ISSQN + IRPJ + INSSPJ + PIS + COFINS + CSLL
#   Mercadoria (01,04)  → PIS + COFINS + CSLL
#   Financeiro (06)     → IOF
#   Outros              → IRPJ + PIS + COFINS
#
# Na query de tesouraria o join é:
#   LEFT JOIN (SELECT IDLAN, CODCOLIGADA, SUM(VALOR) AS VALOR FROM FTRBLAN
#              GROUP BY IDLAN, CODCOLIGADA) AS FTRBLAN
#              ON FLAN.IDLAN = FTRBLAN.IDLAN
#              AND FLAN.CODCOLIGADA = FTRBLAN.CODCOLIGADA
# Isso soma todos os tributos em um único valor deduzido do VL_EFETIVO.
"FTRBLAN": """CREATE TABLE FTRBLAN (
    IDFTRBLAN      INT            NOT NULL,
    IDLAN          INT            NOT NULL,
    CODCOLIGADA    TINYINT        NOT NULL DEFAULT 1,
    CODTRIBUTO     VARCHAR(10)    NOT NULL,  -- ex: 'ISSQN', 'IRPJ', 'PIS'
    DESCRICAO      VARCHAR(60)    NULL,
    ALIQUOTA       DECIMAL(7,4)   NOT NULL,  -- ex: 0.0500 = 5%
    BASE_CALCULO   DECIMAL(15,2)  NOT NULL,
    VALOR          DECIMAL(15,2)  NOT NULL,  -- aliquota × base_calculo
    DT_ATUALIZACAO DATETIME2      NOT NULL,
    CONSTRAINT PK_FTRBLAN      PRIMARY KEY (IDFTRBLAN),
    CONSTRAINT FK_FTRBLAN_FLAN FOREIGN KEY (IDLAN) REFERENCES FLAN(IDLAN)
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
    cur.execute(f"SELECT TOP ({n}) {pk} FROM {tabela} ORDER BY NEWID()")
    return tuple(row[0] for row in cur.fetchall())

def num_nf(idlan: int) -> str:
    """Gera número de NF simulado (6 dígitos, sem parcela — parcela é adicionada pelo chamador)."""
    base = (idlan * 7 + 100000) % 900000 + 100000
    return f"{base:06d}"


# ══════════════════════════════════════════════════════════════════
#  5. Lógica de tributos (FTRBLAN)
# ══════════════════════════════════════════════════════════════════

# Tabela de tributos: código → (descrição, alíquota_base)
TRIBUTOS = {
    "ISSQN":  ("ISS sobre Serviços",          0.0500),
    "IRPJ":   ("IR Retido na Fonte",          0.0150),
    "INSSPJ": ("INSS Pessoa Jurídica",         0.1100),
    "PIS":    ("PIS/Pasep",                   0.0065),
    "COFINS": ("COFINS",                      0.0300),
    "CSLL":   ("Contribuição Social s/ Lucro", 0.0100),
    "IOF":    ("IOF",                         0.0038),
}

def tributos_por_codtdo(codtdo: str) -> list[str]:
    """Retorna lista de tributos incidentes conforme tipo de documento."""
    if codtdo in CODTDO_SERVICO:
        return ["ISSQN", "IRPJ", "INSSPJ", "PIS", "COFINS", "CSLL"]
    elif codtdo in CODTDO_FINANCEIRO:
        return ["IOF"]
    elif codtdo in ("01", "04", "09"):
        # Mercadoria: PIS, COFINS, CSLL (sem ISSQN/INSS)
        return ["PIS", "COFINS", "CSLL"]
    else:
        # Boleto, duplicata, frete, etc.
        return ["IRPJ", "PIS", "COFINS"]

def gerar_ftrblan(flan_rows: list, base_id: int) -> list:
    """
    Gera registros de tributos para cada lançamento da FLAN.

    Apenas ~70% dos lançamentos têm tributos (o restante são pagamentos
    simples sem retenção, como adiantamentos ou documentos não tributáveis).
    Alíquotas sofrem variação de ±20% para simular negociações/regimes.
    """
    rows = []
    idc  = [base_id]

    for lan in flan_rows:
        # Lançamentos cancelados ou sem codtdo não geram tributos
        if lan["STATUSLAN"] == "C" or not lan["CODTDO"]:
            continue
        if random.random() > 0.70:
            continue

        trib_codes = tributos_por_codtdo(lan["CODTDO"])
        base_calc  = float(lan["VALORORIGINAL"])

        for cod in trib_codes:
            desc, aliq_base = TRIBUTOS[cod]
            # Variação ±20% na alíquota para simular regimes diferenciados
            aliq  = round(aliq_base * random.uniform(0.80, 1.20), 4)
            valor = round(base_calc * aliq, 2)
            if valor <= 0:
                continue
            rows.append({
                "IDFTRBLAN":      nxt(idc),
                "IDLAN":          lan["IDLAN"],
                "CODCOLIGADA":    lan["CODCOLIGADA"],
                "CODTRIBUTO":     cod,
                "DESCRICAO":      desc,
                "ALIQUOTA":       aliq,
                "BASE_CALCULO":   base_calc,
                "VALOR":          valor,
                "DT_ATUALIZACAO": lan["DT_ATUALIZACAO"],
            })

    return rows


# ══════════════════════════════════════════════════════════════════
#  6. Geração de dados dimensionais
# ══════════════════════════════════════════════════════════════════
def gerar_produtos(n: int, ts: datetime) -> list:
    return [{
        "CODPROD": i, "NOMEPROD": fake.bs().title()[:100],
        "CATEGORIA": random.choice(CATEGORIAS), "UNIDADE": random.choice(UNIDADES),
        "PRECO_BASE": round(random.uniform(5, 8000), 2),
        "ATIVO": random.choices([1, 0], weights=[95, 5])[0],
        "DT_ATUALIZACAO": ts,
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
            "CODCFO": i,
            "NOMECFO": fake.company()[:120],
            "TIPOCFO": random.choices(["F", "A"], weights=[80, 20])[0],
            "CNPJ_CPF": cnpj,
            "EMAIL": fake.company_email() if random.random() > 0.2 else None,
            "TELEFONE": fake.phone_number()[:20] if random.random() > 0.3 else None,
            "CIDADE": fake.city()[:60],
            "UF": fake.estado_sigla(),
            "ATIVO": random.choices([1, 0], weights=[92, 8])[0],
            "DT_ATUALIZACAO": ts,
        })
    return rows

def gerar_gccusto(ts: datetime) -> list:
    return [{
        "CODCCUSTO": cc,
        "NOME": f"Centro {cc} — {random.choice(TIPOS_CCUSTO)}",
        "TIPO": random.choice(TIPOS_CCUSTO),
        "RESPONSAVEL": fake.name()[:80],
        "ATIVO": 1,
        "DT_ATUALIZACAO": ts,
    } for cc in CENTROS]

def gerar_tloc(ts: datetime) -> list:
    rows = []
    for i in range(1, NUM_LOCAIS + 1):
        fil = random.choice(FILIAIS)
        tipo = random.choice(TIPOS_LOC)
        rows.append({
            "IDLOC": i,
            "NOME": f"Almox. {tipo} — Filial {fil}",
            "FILIAL": fil,
            "TIPO": tipo,
            "ATIVO": random.choices([1, 0], weights=[93, 7])[0],
            "DT_ATUALIZACAO": ts,
        })
    return rows

def gerar_fctdo(ts: datetime) -> list:
    return [{
        "CODTDO":         r[0],
        "DESCRICAO":      r[1],
        "CLASSIFIC":      r[2],
        "OBSERVACAO":     r[3],
        "DT_ATUALIZACAO": ts,
    } for r in FCTDO_DATA]


# ══════════════════════════════════════════════════════════════════
#  7. Geração de FLAN + FLANBAIXA (módulo financeiro expandido)
# ══════════════════════════════════════════════════════════════════
def gerar_flan_e_baixas(
    tmov_recs: list,
    ts: datetime,
    base_idlan: int,
    base_idbaixa: int,
) -> tuple[list, list]:
    """
    Gera FLAN (schema RM completo) e FLANBAIXA (baixas parciais + quitação).

    Regras de simulação:
    ─────────────────────────────────────────────────────────────────
    FLAN
      • Cada REC não-Devolvido gera 1..3 parcelas
      • Número de documento: '{nf}/{parcela:02d}'  ex: '042891/01'
      • ~5% dos lançamentos são cancelados (STATUSLAN='C')
      • VALOROP1/2/3 acumulam os tributos principais por lançamento
        (ISSQN, IRPJ, INSSPJ) diretamente na FLAN, além da FTRBLAN
      • VALORDESCONTO, VALORJUROS, VALORMULTA simulados para
        documentos pagos dentro e fora do prazo
      • DATAPREVBAIXA = DATAVENCIMENTO - 0..3 dias (agenda tesouraria)

    FLANBAIXA
      • Lançamentos 'P' (pagos) têm 1..3 baixas
        – se 1 baixa: valor total de uma vez (quitação direta)
        – se 2+ baixas: parciais distribuídas aleatoriamente, última
          completa o saldo (garante consistência com VALORORIGINAL)
      • VALORBAIXADO na FLAN = soma das baixas geradas
      • ~3% dos pagos têm DATACANCELBAIXA (baixa cancelada)
    ─────────────────────────────────────────────────────────────────
    """
    hoje       = datetime.now().date()
    flan_rows  = []
    baixa_rows = []
    idlan      = [base_idlan]
    idbaixa    = [base_idbaixa]

    for rec in tmov_recs:
        if rec["STATUS"] == "Devolvido":
            continue

        codtdo      = random.choices(CODTDO_COMPRA, weights=CODTDO_PESOS)[0]
        n_parcelas  = random.choices([1, 2, 3], weights=[55, 30, 15])[0]
        valor_total = float(rec["VALORTOTAL"])
        valor_parc  = round(valor_total / n_parcelas, 2)
        dt_base     = rec["DATAEMISSAO"]        # já é date
        filial_cod  = random.choice(FILIAIS_CODIGO)
        nf_num      = num_nf(idlan[0] + 1)

        for parc in range(1, n_parcelas + 1):
            lan_id   = nxt(idlan)
            venc     = dt_base + timedelta(days=30 * parc)
            cancelado = random.random() < 0.05

            # Cálculo de tributos diretos na FLAN (VALOROP1/2/3)
            # Reflete o que o RM armazena no próprio lançamento como deduções principais
            if codtdo in CODTDO_SERVICO:
                vop1 = round(valor_parc * 0.0500, 2)   # ISSQN
                vop2 = round(valor_parc * 0.0150, 2)   # IRPJ
                vop3 = round(valor_parc * 0.1100, 2)   # INSSPJ
            else:
                vop1, vop2, vop3 = 0.0, 0.0, 0.0

            # Pagamento — somente se não cancelado e vencimento no passado
            vencido = venc < hoje
            pago    = not cancelado and vencido and random.random() < 0.75

            # Multa/juros apenas em pagamentos em atraso
            if pago:
                dias_atraso = random.randint(0, 45)
                datapag     = venc + timedelta(days=dias_atraso)
                desconto    = round(valor_parc * random.uniform(0, 0.02), 2) if dias_atraso == 0 else 0.0
                juros       = round(valor_parc * 0.001 * dias_atraso, 2) if dias_atraso > 0 else 0.0
                multa       = round(valor_parc * 0.02, 2) if dias_atraso > 3 else 0.0
            else:
                datapag, desconto, juros, multa = None, 0.0, 0.0, 0.0

            # Cancelamento de baixa: ~3% dos pagos
            cancela_baixa = pago and random.random() < 0.03
            dt_cancelbaixa = (datapag + timedelta(days=random.randint(1, 10))
                              if cancela_baixa else None)
            # Se baixa foi cancelada, trata como não-pago para fins de status
            status_efetivo_pago = pago and not cancela_baixa

            statuslan = ("C" if cancelado
                         else ("P" if status_efetivo_pago else "A"))

            # ── Baixas (FLANBAIXA) ──────────────────────────────
            # Gera baixas para pagamentos efetivos (não cancelados)
            baixas_geradas = []
            if pago and not cancela_baixa:
                n_baixas = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
                if n_baixas == 1:
                    baixas_geradas = [(datapag, valor_parc)]
                else:
                    # Baixas parciais: divide aleatoriamente, último completa o saldo
                    saldo = valor_parc
                    for b in range(1, n_baixas):
                        frac     = round(saldo * random.uniform(0.20, 0.60), 2)
                        dt_b     = datapag - timedelta(days=(n_baixas - b) * random.randint(1, 7))
                        baixas_geradas.append((dt_b, frac))
                        saldo    = round(saldo - frac, 2)
                    baixas_geradas.append((datapag, saldo))  # última quita o saldo

            valorbaixado_total = sum(v for _, v in baixas_geradas)

            flan_rows.append({
                "IDLAN":             lan_id,
                "CODCOLIGADA":       CODCOLIGADA,
                "CODFILIAL":         filial_cod,
                "IDMOV":             rec["IDMOV"],
                "CODFORNEC":         rec["CODFORNEC"],
                "CODTDO":            codtdo,
                "NUMERODOCUMENTO":   f"{nf_num}/{parc:02d}",
                "NUMPARCELA":        parc,
                "TOTPARCELAS":       n_parcelas,
                "PAGREC":            2,
                "DATAEMISSAO":       dt_base,
                "DATAVENCIMENTO":    venc,
                "DATAPREVBAIXA":     venc - timedelta(days=random.randint(0, 3)),
                "DATAPAG":           datapag if status_efetivo_pago else None,
                "DATACANCELAMENTO":  dt_base + timedelta(days=random.randint(1, 5)) if cancelado else None,
                "DATACANCELBAIXA":   dt_cancelbaixa,
                "VALORORIGINAL":     valor_parc,
                "VALOROP1":          vop1,
                "VALOROP2":          vop2,
                "VALOROP3":          vop3,
                "VALORDESCONTO":     desconto,
                "VALORJUROS":        juros,
                "VALORMULTA":        multa,
                "VALORBAIXADO":      valorbaixado_total,
                "STATUSLAN":         statuslan,
                "HISTORICO":         f"Parcela {parc}/{n_parcelas} — NF {nf_num} — {rec['NUMEROMOV']}",
                "CODCCUSTO":         rec.get("CODCCUSTO"),
                "DT_ATUALIZACAO":    ts,
            })

            for dt_b, val_b in baixas_geradas:
                baixa_rows.append({
                    "IDBAIXA":        nxt(idbaixa),
                    "IDLAN":          lan_id,
                    "CODCOLIGADA":    CODCOLIGADA,
                    "CODFILIAL":      filial_cod,
                    "DATABAIXA":      dt_b,
                    "VALORBAIXADO":   val_b,
                    "HISTORICO":      f"Baixa — IDLAN {lan_id} — {val_b:.2f}",
                    "DT_ATUALIZACAO": ts,
                })

    return flan_rows, baixa_rows


# ══════════════════════════════════════════════════════════════════
#  8. Geração de movimentos (SC → OC → REC) — inalterado
# ══════════════════════════════════════════════════════════════════
def gerar_movimentos(produtos, fcfo_rows, locais, ts, base_mov, base_itm, base_rel, base_irel):
    tmov, titmmov, tmovrelac, titmmovrelac = [], [], [], []
    idx: dict[tuple, dict] = {}

    mc  = [base_mov]
    ic  = [base_itm]
    rc  = [base_rel]
    irc = [base_irel]

    loc_ids = [l["IDLOC"] for l in locais if l["ATIVO"]]

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
            price = round(p["PRECO_BASE"] * random.uniform(0.88, 1.12), 2)
            iid   = nxt(ic)
            r = {
                "IDITMOV": iid, "IDMOV": sc_id, "NITEM": nitem,
                "CODPROD": p["CODPROD"], "IDLOC": None,
                "QUANTIDADE": qty, "VALORUNIT": price,
                "VALORTOTAL": round(qty * price, 2), "UNIDADE": p["UNIDADE"],
                "OBSERVACAO": fake.sentence(nb_words=5)[:300] if random.random() < 0.25 else "",
                "DT_ATUALIZACAO": ts,
            }
            titmmov.append(r)
            idx[(sc_id, nitem)] = r
            sc_total += r["VALORTOTAL"]
            sc_nitens.append(nitem)

        tmov.append({
            "IDMOV": sc_id, "CODTMV": random.choice(CODTMV_SC),
            "NUMEROMOV": f"SC{sc_id:08d}",
            "DATAEMISSAO": sc_dt.date(), "DATAENTREGA": None,
            "DATACOMPETENCIA": competencia(sc_dt).date(),
            "CODFORNEC": None, "FILIAL": sc_fil, "CODCCUSTO": sc_cc,
            "STATUS": random.choices(STATUS_SC, weights=[10, 20, 60, 10])[0],
            "VALORTOTAL": round(sc_total, 2), "DESCONTO": 0.0, "FRETE": 0.0,
            "OBSERVACAO": fake.sentence(nb_words=9)[:500] if random.random() < 0.35 else "",
            "USUARIOCRIACAO": sc_user, "DATACRIACAO": sc_dt, "DT_ATUALIZACAO": ts,
        })

        for _ in range(random.randint(*OCs_POR_SC)):
            oc_dt   = sc_dt + timedelta(days=random.randint(1, 20))
            oc_id   = nxt(mc)
            forn    = random.choice(fcfo_rows)
            oc_user = random.choice(USUARIOS)

            sub_sc_nitens = random.sample(sc_nitens, k=random.randint(1, len(sc_nitens)))

            tmovrelac.append({
                "IDMOVRELAC": nxt(rc),
                "IDMOV_ORIG": sc_id, "IDMOV_DEST": oc_id,
                "TIPO_ORIG": "SC", "TIPO_DEST": "OC",
                "DT_ATUALIZACAO": ts,
            })

            oc_total  = 0.0
            oc_nitens = []

            for nitem in sub_sc_nitens:
                orig  = idx[(sc_id, nitem)]
                qty   = round(orig["QUANTIDADE"] * random.uniform(0.5, 1.0), 3)
                price = round(orig["VALORUNIT"]  * random.uniform(0.94, 1.06), 2)
                iid   = nxt(ic)
                r = {
                    "IDITMOV": iid, "IDMOV": oc_id, "NITEM": nitem,
                    "CODPROD": orig["CODPROD"], "IDLOC": None,
                    "QUANTIDADE": qty, "VALORUNIT": price,
                    "VALORTOTAL": round(qty * price, 2), "UNIDADE": orig["UNIDADE"],
                    "OBSERVACAO": "", "DT_ATUALIZACAO": ts,
                }
                titmmov.append(r)
                idx[(oc_id, nitem)] = r
                oc_total += r["VALORTOTAL"]
                oc_nitens.append(nitem)

                titmmovrelac.append({
                    "IDITMMOVRELAC": nxt(irc),
                    "IDMOV_ORIG": sc_id, "NITEM_ORIG": nitem,
                    "IDMOV_DEST": oc_id, "NITEM_DEST": nitem,
                    "DT_ATUALIZACAO": ts,
                })

            desc  = round(oc_total * random.uniform(0, 0.05), 2)
            frete = round(random.uniform(0, 350), 2)
            tmov.append({
                "IDMOV": oc_id, "CODTMV": random.choice(CODTMV_OC),
                "NUMEROMOV": f"OC{oc_id:08d}",
                "DATAEMISSAO": oc_dt.date(),
                "DATAENTREGA": (oc_dt + timedelta(days=random.randint(7, 45))).date(),
                "DATACOMPETENCIA": competencia(oc_dt).date(),
                "CODFORNEC": forn["CODCFO"], "FILIAL": sc_fil, "CODCCUSTO": sc_cc,
                "STATUS": random.choices(STATUS_OC, weights=[15, 20, 55, 10])[0],
                "VALORTOTAL": round(oc_total - desc + frete, 2),
                "DESCONTO": desc, "FRETE": frete, "OBSERVACAO": "",
                "USUARIOCRIACAO": oc_user, "DATACRIACAO": oc_dt, "DT_ATUALIZACAO": ts,
            })

            for _ in range(random.randint(*RECs_POR_OC)):
                rec_dt   = oc_dt + timedelta(days=random.randint(5, 60))
                rec_id   = nxt(mc)
                rec_user = random.choice(USUARIOS)

                sub_oc_nitens = random.sample(oc_nitens, k=random.randint(1, len(oc_nitens)))

                tmovrelac.append({
                    "IDMOVRELAC": nxt(rc),
                    "IDMOV_ORIG": oc_id, "IDMOV_DEST": rec_id,
                    "TIPO_ORIG": "OC", "TIPO_DEST": "REC",
                    "DT_ATUALIZACAO": ts,
                })

                rec_total = 0.0

                for nitem in sub_oc_nitens:
                    orig2 = idx[(oc_id, nitem)]
                    qty   = round(orig2["QUANTIDADE"] * random.uniform(0.3, 1.0), 3)
                    iid   = nxt(ic)
                    r2 = {
                        "IDITMOV": iid, "IDMOV": rec_id, "NITEM": nitem,
                        "CODPROD": orig2["CODPROD"],
                        "IDLOC": random.choice(loc_ids),
                        "QUANTIDADE": qty, "VALORUNIT": orig2["VALORUNIT"],
                        "VALORTOTAL": round(qty * orig2["VALORUNIT"], 2),
                        "UNIDADE": orig2["UNIDADE"], "OBSERVACAO": "",
                        "DT_ATUALIZACAO": ts,
                    }
                    titmmov.append(r2)
                    idx[(rec_id, nitem)] = r2
                    rec_total += r2["VALORTOTAL"]

                    titmmovrelac.append({
                        "IDITMMOVRELAC": nxt(irc),
                        "IDMOV_ORIG": oc_id,  "NITEM_ORIG": nitem,
                        "IDMOV_DEST": rec_id, "NITEM_DEST": nitem,
                        "DT_ATUALIZACAO": ts,
                    })

                status_rec = random.choices(STATUS_REC, weights=[70, 20, 10])[0]
                tmov.append({
                    "IDMOV": rec_id, "CODTMV": random.choice(CODTMV_REC),
                    "NUMEROMOV": f"REC{rec_id:08d}",
                    "DATAEMISSAO": rec_dt.date(), "DATAENTREGA": rec_dt.date(),
                    "DATACOMPETENCIA": competencia(rec_dt).date(),
                    "CODFORNEC": forn["CODCFO"], "FILIAL": sc_fil, "CODCCUSTO": sc_cc,
                    "STATUS": status_rec,
                    "VALORTOTAL": round(rec_total, 2), "DESCONTO": 0.0, "FRETE": 0.0,
                    "OBSERVACAO": "",
                    "USUARIOCRIACAO": rec_user, "DATACRIACAO": rec_dt,
                    "DT_ATUALIZACAO": ts,
                })

    return tmov, titmmov, tmovrelac, titmmovrelac


# ══════════════════════════════════════════════════════════════════
#  9. UPDATE — simulação de re-feed
# ══════════════════════════════════════════════════════════════════
def aplicar_updates(cur, ts: datetime, total_tmov: int):
    print("\n🔄  Aplicando UPDATEs (simulação de re-feed do sistema fonte)...")
    hoje = datetime.now().date()

    # ── TMOV ─────────────────────────────────────────────────────
    n = max(1, int(total_tmov * OVERLAP_PERC))
    cur.execute(f"SELECT TOP ({n}) IDMOV, STATUS, VALORTOTAL FROM TMOV ORDER BY NEWID()")
    rows_tmov = cur.fetchall()
    if rows_tmov:
        upd = [(proximo_status(s), round(float(v or 0) * random.uniform(0.97, 1.03), 2),
                fake.sentence(nb_words=7)[:500] if random.random() < 0.5 else "", ts, i)
               for i, s, v in rows_tmov]
        cur.executemany("""
            UPDATE TMOV SET STATUS=?, VALORTOTAL=?, OBSERVACAO=?, DT_ATUALIZACAO=?
            WHERE IDMOV=?
        """, upd)
        print(f"  ✔ TMOV            {len(upd):>6,} atualizados  (STATUS, VALORTOTAL, OBSERVACAO)")

    # ── TITMMOV ──────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM TITMMOV")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute(f"SELECT TOP ({n}) IDITMOV, QUANTIDADE, VALORUNIT FROM TITMMOV ORDER BY NEWID()")
    rows_titm = cur.fetchall()
    if rows_titm:
        upd = []
        for iditm, qtd, vunit in rows_titm:
            nq = round(float(qtd or 1) * random.uniform(0.95, 1.05), 3)
            upd.append((nq, round(nq * float(vunit or 0), 2), ts, iditm))
        cur.executemany("""
            UPDATE TITMMOV SET QUANTIDADE=?, VALORTOTAL=?, DT_ATUALIZACAO=?
            WHERE IDITMOV=?
        """, upd)
        print(f"  ✔ TITMMOV         {len(upd):>6,} atualizados  (QUANTIDADE, VALORTOTAL)")

    # ── PPRODUTO ─────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM PPRODUTO")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute(f"SELECT TOP ({n}) CODPROD, PRECO_BASE, ATIVO FROM PPRODUTO ORDER BY NEWID()")
    rows_prod = cur.fetchall()
    if rows_prod:
        upd = [(round(float(p or 1) * random.uniform(0.92, 1.08), 2),
                (1 - a) if random.random() < 0.02 else a, ts, c)
               for c, p, a in rows_prod]
        cur.executemany("""
            UPDATE PPRODUTO SET PRECO_BASE=?, ATIVO=?, DT_ATUALIZACAO=?
            WHERE CODPROD=?
        """, upd)
        print(f"  ✔ PPRODUTO        {len(upd):>6,} atualizados  (PRECO_BASE, ATIVO)")

    # ── TMOVRELAC ────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM TMOVRELAC")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    ids = sample_ids(cur, "TMOVRELAC", "IDMOVRELAC", n)
    if ids:
        cur.executemany("UPDATE TMOVRELAC SET DT_ATUALIZACAO=? WHERE IDMOVRELAC=?",
                        [(ts, i) for i in ids])
        print(f"  ✔ TMOVRELAC       {len(ids):>6,} atualizados  (DT_ATUALIZACAO)")

    # ── TITMMOVRELAC ─────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM TITMMOVRELAC")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    ids = sample_ids(cur, "TITMMOVRELAC", "IDITMMOVRELAC", n)
    if ids:
        cur.executemany("UPDATE TITMMOVRELAC SET DT_ATUALIZACAO=? WHERE IDITMMOVRELAC=?",
                        [(ts, i) for i in ids])
        print(f"  ✔ TITMMOVRELAC    {len(ids):>6,} atualizados  (DT_ATUALIZACAO)")

    # ── FCFO ─────────────────────────────────────────────────────
    cur.execute("SELECT COUNT(*) FROM FCFO")
    n = max(1, int(cur.fetchone()[0] * OVERLAP_PERC))
    cur.execute(f"SELECT TOP ({n}) CODCFO, ATIVO FROM FCFO ORDER BY NEWID()")
    rows_fcfo = cur.fetchall()
    if rows_fcfo:
        upd = [((1 - a) if random.random() < 0.03 else a, ts, c) for c, a in rows_fcfo]
        cur.executemany("UPDATE FCFO SET ATIVO=?, DT_ATUALIZACAO=? WHERE CODCFO=?", upd)
        print(f"  ✔ FCFO            {len(upd):>6,} atualizados  (ATIVO)")

    # ── FLAN — simula pagamentos de parcelas vencidas em aberto ──
    cur.execute("""
        SELECT TOP (500) IDLAN, VALORORIGINAL, VALORBAIXADO, CODFILIAL, CODCOLIGADA
        FROM FLAN
        WHERE STATUSLAN = 'A'
          AND DATAVENCIMENTO < CAST(GETDATE() AS DATE)
          AND DATACANCELAMENTO IS NULL
        ORDER BY NEWID()
    """)
    rows_flan = cur.fetchall()
    flan_upd  = []
    baixa_upd = []

    cur.execute("SELECT ISNULL(MAX(IDBAIXA), 0) FROM FLANBAIXA")
    max_baixa = [cur.fetchone()[0]]

    for idlan, voriginal, vbaixado, codfilial, codcol in rows_flan:
        if random.random() > 0.40:
            continue
        dt_pag    = hoje + timedelta(days=random.randint(0, 5))
        saldo     = round(float(voriginal or 0) - float(vbaixado or 0), 2)
        if saldo <= 0:
            continue
        juros     = round(saldo * 0.001 * random.randint(0, 30), 2)
        multa     = round(saldo * 0.02, 2) if random.random() < 0.4 else 0.0
        flan_upd.append((
            dt_pag, saldo, juros, multa,
            round(float(vbaixado or 0) + saldo, 2),
            ts, idlan
        ))
        baixa_upd.append({
            "IDBAIXA":        nxt(max_baixa),
            "IDLAN":          idlan,
            "CODCOLIGADA":    codcol,
            "CODFILIAL":      codfilial,
            "DATABAIXA":      dt_pag,
            "VALORBAIXADO":   saldo,
            "HISTORICO":      f"Quitação via update — IDLAN {idlan}",
            "DT_ATUALIZACAO": ts,
        })

    if flan_upd:
        cur.executemany("""
            UPDATE FLAN
            SET DATAPAG=?, 
                VALORORIGINAL=?, -- Se 'saldo' for o novo valor original, ou ajuste a ordem
                VALORJUROS=?, 
                VALORMULTA=?,
                VALORBAIXADO=?, 
                STATUSLAN='P', 
                DT_ATUALIZACAO=?
            WHERE IDLAN=?
        """, flan_upd)
        print(f"  ✔ FLAN            {len(flan_upd):>6,} atualizados  (STATUSLAN→'P', DATAPAG, juros/multa)")

    if baixa_upd:
        insert_batch(cur, "FLANBAIXA",
            ["IDBAIXA","IDLAN","CODCOLIGADA","CODFILIAL","DATABAIXA","VALORBAIXADO",
             "HISTORICO","DT_ATUALIZACAO"],
            baixa_upd)

    # ── FTRBLAN — re-calcula tributos de lançamentos pagos ───────
    cur.execute("""
        SELECT TOP (200) F.IDLAN, F.CODCOLIGADA, F.VALORORIGINAL, F.CODTDO
        FROM FLAN F
        WHERE F.STATUSLAN = 'P'
          AND NOT EXISTS (SELECT 1 FROM FTRBLAN T WHERE T.IDLAN = F.IDLAN)
        ORDER BY NEWID()
    """)
    rows_trib = cur.fetchall()
    if rows_trib:
        cur.execute("SELECT ISNULL(MAX(IDFTRBLAN), 0) FROM FTRBLAN")
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
                    "IDFTRBLAN":      nxt(max_trib),
                    "IDLAN":          idlan,
                    "CODCOLIGADA":    codcol,
                    "CODTRIBUTO":     cod,
                    "DESCRICAO":      desc,
                    "ALIQUOTA":       aliq,
                    "BASE_CALCULO":   float(voriginal or 0),
                    "VALOR":          valor,
                    "DT_ATUALIZACAO": ts,
                })
        if trib_rows:
            insert_batch(cur, "FTRBLAN",
                ["IDFTRBLAN","IDLAN","CODCOLIGADA","CODTRIBUTO","DESCRICAO",
                 "ALIQUOTA","BASE_CALCULO","VALOR","DT_ATUALIZACAO"],
                trib_rows)
            print(f"  ✔ FTRBLAN         {len(trib_rows):>6,} inseridos    (tributos de pagamentos novos)")


# ══════════════════════════════════════════════════════════════════
#  10. Infraestrutura SQL Server
# ══════════════════════════════════════════════════════════════════
def conn_str(db="master"):
    s = f"DRIVER={{{SQL_DRIVER}}};SERVER={SQL_SERVER};DATABASE={db};"
    return s + ("Trusted_Connection=yes;" if SQL_TRUSTED else f"UID={SQL_USER};PWD={SQL_PWD};")

def conectar(db="master"):
    return pyodbc.connect(conn_str(db), autocommit=True)

def criar_banco(conn):
    conn.cursor().execute(f"""
        IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{SQL_DATABASE}')
        CREATE DATABASE [{SQL_DATABASE}]
    """)
    print(f"  ✔ Banco [{SQL_DATABASE}] verificado/criado.")

def criar_tabelas(conn):
    cur = conn.cursor()
    ordem_drop   = ["FTRBLAN","FLANBAIXA","FLAN","TITMMOVRELAC","TITMMOV",
                    "TMOVRELAC","TMOV","PPRODUTO","TLOC","GCCUSTO","FCFO","FCTDO","TTMV"]
    ordem_create = ["TTMV","FCTDO","FCFO","GCCUSTO","TLOC","PPRODUTO",
                    "TMOV","TITMMOV","TMOVRELAC","TITMMOVRELAC",
                    "FLAN","FLANBAIXA","FTRBLAN"]

    if DROP_RECREATE:
        print("  ⚙  DROP_RECREATE=True — removendo tabelas...")
        for t in ordem_drop:
            cur.execute(f"IF OBJECT_ID('{t}','U') IS NOT NULL DROP TABLE {t}")
        conn.commit()

    print("  ⚙  Criando tabelas (se não existirem)...")
    for t in ordem_create:
        cur.execute(f"IF OBJECT_ID('{t}','U') IS NULL EXEC(?)", DDL_TABLES[t])
    conn.commit()
    print("  ✔ Tabelas prontas.")

def ler_max_ids(cur) -> dict:
    def mx(tabela, col):
        cur.execute(f"SELECT ISNULL(MAX({col}), 0) FROM {tabela}")
        return cur.fetchone()[0]
    return {
        "mov":    mx("TMOV",         "IDMOV"),
        "itm":    mx("TITMMOV",      "IDITMOV"),
        "rel":    mx("TMOVRELAC",    "IDMOVRELAC"),
        "irel":   mx("TITMMOVRELAC", "IDITMMOVRELAC"),
        "lan":    mx("FLAN",         "IDLAN"),
        "baixa":  mx("FLANBAIXA",    "IDBAIXA"),
        "tributo":mx("FTRBLAN",      "IDFTRBLAN"),
    }

def insert_batch(cur, tabela, colunas, dados):
    if not dados:
        return
    ph  = ",".join(["?"] * len(colunas))
    sql = f"INSERT INTO {tabela} ({','.join(colunas)}) VALUES ({ph})"
    rows = [[r[c] for c in colunas] for r in dados]
    for i in range(0, len(rows), BATCH_SIZE):
        cur.executemany(sql, rows[i:i+BATCH_SIZE])
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

    print(f"\n🔌  Conectando em [{SQL_SERVER}]...")
    try:
        conn_m = conectar("master")
    except Exception as e:
        print(f"\n❌  Erro de conexão: {e}")
        print("     Verifique SQL_SERVER, SQL_DRIVER e autenticação no CONFIG.")
        return

    criar_banco(conn_m)
    conn_m.close()

    conn = conectar(SQL_DATABASE)
    criar_tabelas(conn)
    cur  = conn.cursor()
    cur.fast_executemany = True

    ids      = ler_max_ids(cur)
    primeira = (ids["mov"] == 0)

    print(f"\n  📌 Estado atual do banco:")
    print(f"     MAX IDMOV          = {ids['mov']:,}")
    print(f"     MAX IDITMOV        = {ids['itm']:,}")
    print(f"     MAX IDMOVRELAC     = {ids['rel']:,}")
    print(f"     MAX IDITMMOVRELAC  = {ids['irel']:,}")
    print(f"     MAX IDLAN          = {ids['lan']:,}")
    print(f"     MAX IDBAIXA        = {ids['baixa']:,}")
    print(f"     MAX IDFTRBLAN      = {ids['tributo']:,}")
    print(f"     Modo               = {'PRIMEIRA CARGA' if primeira else 'INCREMENTAL + UPDATE'}")

    print(f"\n🔧  Gerando {NUM_SCs} novas SCs e cadeia OC → REC...")

    if primeira:
        produtos  = gerar_produtos(NUM_PRODUTOS, ts)
        fcfo_rows = gerar_fcfo(ts)
        gccusto   = gerar_gccusto(ts)
        locais    = gerar_tloc(ts)
        fctdo     = gerar_fctdo(ts)
    else:
        cur.execute("SELECT CODPROD, PRECO_BASE, UNIDADE FROM PPRODUTO WHERE ATIVO = 1")
        produtos = [{"CODPROD": r[0], "PRECO_BASE": float(r[1] or 100), "UNIDADE": r[2] or "UN"}
                    for r in cur.fetchall()]
        cur.execute("SELECT CODCFO FROM FCFO WHERE ATIVO = 1")
        fcfo_rows = [{"CODCFO": r[0]} for r in cur.fetchall()]
        cur.execute("SELECT IDLOC FROM TLOC WHERE ATIVO = 1")
        locais = [{"IDLOC": r[0], "ATIVO": 1} for r in cur.fetchall()]
        gccusto = []
        fctdo   = []

    tmov, titmmov, tmovrelac, titmmovrelac = gerar_movimentos(
        produtos, fcfo_rows, locais, ts,
        ids["mov"], ids["itm"], ids["rel"], ids["irel"]
    )

    # Inclui CODCCUSTO no dicionário dos RECs para herdar na FLAN
    tmov_map = {m["IDMOV"]: m for m in tmov}
    tmov_recs = [m for m in tmov if m["CODTMV"] in CODTMV_REC]

    flan_rows, baixa_rows = gerar_flan_e_baixas(
        tmov_recs, ts, ids["lan"], ids["baixa"]
    )

    ftrblan_rows = gerar_ftrblan(flan_rows, ids["tributo"])

    sc_ct  = sum(1 for m in tmov if m["CODTMV"] in CODTMV_SC)
    oc_ct  = sum(1 for m in tmov if m["CODTMV"] in CODTMV_OC)
    rec_ct = len(tmov_recs)
    pago_ct = sum(1 for f in flan_rows if f["STATUSLAN"] == "P")
    canc_ct = sum(1 for f in flan_rows if f["STATUSLAN"] == "C")

    print(f"\n  📊 Novos registros gerados:")
    print(f"     TMOV        → SC:{sc_ct:,}  OC:{oc_ct:,}  REC:{rec_ct:,}  Total:{len(tmov):,}")
    print(f"     TITMMOV     → {len(titmmov):,}")
    print(f"     TMOVRELAC   → {len(tmovrelac):,}")
    print(f"     TITMMOVRELAC→ {len(titmmovrelac):,}")
    print(f"     FLAN        → {len(flan_rows):,}  (Pago:{pago_ct:,}  Cancelado:{canc_ct:,}  Aberto:{len(flan_rows)-pago_ct-canc_ct:,})")
    print(f"     FLANBAIXA   → {len(baixa_rows):,}  (baixas parciais + quitações)")
    print(f"     FTRBLAN     → {len(ftrblan_rows):,}  (ISSQN/IRPJ/INSSPJ/PIS/COFINS/CSLL/IOF)")

    print(f"\n📥  Inserindo novos registros...")

    if primeira:
        insert_batch(cur, "TTMV",
            ["CODTMV","NOME","TIPO","DESCRICAO","DT_ATUALIZACAO"],
            [{"CODTMV":r[0],"NOME":r[1],"TIPO":r[2],"DESCRICAO":r[3],"DT_ATUALIZACAO":ts}
             for r in TTMV_DATA])
        conn.commit()
        insert_batch(cur, "FCTDO",
            ["CODTDO","DESCRICAO","CLASSIFIC","OBSERVACAO","DT_ATUALIZACAO"],
            fctdo)
        conn.commit()
        insert_batch(cur, "FCFO",
            ["CODCFO","NOMECFO","TIPOCFO","CNPJ_CPF","EMAIL","TELEFONE",
             "CIDADE","UF","ATIVO","DT_ATUALIZACAO"],
            fcfo_rows)
        conn.commit()
        insert_batch(cur, "GCCUSTO",
            ["CODCCUSTO","NOME","TIPO","RESPONSAVEL","ATIVO","DT_ATUALIZACAO"],
            gccusto)
        conn.commit()
        insert_batch(cur, "TLOC",
            ["IDLOC","NOME","FILIAL","TIPO","ATIVO","DT_ATUALIZACAO"],
            locais)
        conn.commit()
        insert_batch(cur, "PPRODUTO",
            ["CODPROD","NOMEPROD","CATEGORIA","UNIDADE","PRECO_BASE","ATIVO","DT_ATUALIZACAO"],
            produtos)
        conn.commit()

    insert_batch(cur, "TMOV",
        ["IDMOV","CODTMV","NUMEROMOV","DATAEMISSAO","DATAENTREGA","DATACOMPETENCIA",
         "CODFORNEC","FILIAL","CODCCUSTO","STATUS",
         "VALORTOTAL","DESCONTO","FRETE","OBSERVACAO",
         "USUARIOCRIACAO","DATACRIACAO","DT_ATUALIZACAO"],
        tmov)
    conn.commit()

    insert_batch(cur, "TITMMOV",
        ["IDITMOV","IDMOV","NITEM","CODPROD","IDLOC","QUANTIDADE","VALORUNIT",
         "VALORTOTAL","UNIDADE","OBSERVACAO","DT_ATUALIZACAO"],
        titmmov)
    conn.commit()

    insert_batch(cur, "TMOVRELAC",
        ["IDMOVRELAC","IDMOV_ORIG","IDMOV_DEST","TIPO_ORIG","TIPO_DEST","DT_ATUALIZACAO"],
        tmovrelac)
    conn.commit()

    insert_batch(cur, "TITMMOVRELAC",
        ["IDITMMOVRELAC","IDMOV_ORIG","NITEM_ORIG","IDMOV_DEST","NITEM_DEST","DT_ATUALIZACAO"],
        titmmovrelac)
    conn.commit()

    insert_batch(cur, "FLAN",
        ["IDLAN","CODCOLIGADA","CODFILIAL","IDMOV","CODFORNEC","CODTDO",
         "NUMERODOCUMENTO","NUMPARCELA","TOTPARCELAS","PAGREC",
         "DATAEMISSAO","DATAVENCIMENTO","DATAPREVBAIXA","DATAPAG",
         "DATACANCELAMENTO","DATACANCELBAIXA",
         "VALORORIGINAL","VALOROP1","VALOROP2","VALOROP3",
         "VALORDESCONTO","VALORJUROS","VALORMULTA","VALORBAIXADO",
         "STATUSLAN","HISTORICO","CODCCUSTO","DT_ATUALIZACAO"],
        flan_rows)
    conn.commit()

    insert_batch(cur, "FLANBAIXA",
        ["IDBAIXA","IDLAN","CODCOLIGADA","CODFILIAL","DATABAIXA","VALORBAIXADO",
         "HISTORICO","DT_ATUALIZACAO"],
        baixa_rows)
    conn.commit()

    insert_batch(cur, "FTRBLAN",
        ["IDFTRBLAN","IDLAN","CODCOLIGADA","CODTRIBUTO","DESCRICAO",
         "ALIQUOTA","BASE_CALCULO","VALOR","DT_ATUALIZACAO"],
        ftrblan_rows)
    conn.commit()

    if not primeira:
        aplicar_updates(cur, ts, ids["mov"])
        conn.commit()
    else:
        print("\n  ℹ️  Primeira execução — UPDATEs pulados.")
        print("     Mude DROP_RECREATE=False e rode novamente para simular o re-feed.")

    conn.close()

    conn2 = conectar(SQL_DATABASE)
    cur2  = conn2.cursor()
    print(f"\n{'─'*64}")
    print(f"  📦 Totais acumulados no banco:")
    for t in ["TMOV","TITMMOV","TMOVRELAC","TITMMOVRELAC",
              "PPRODUTO","FCFO","GCCUSTO","TLOC","FCTDO",
              "FLAN","FLANBAIXA","FTRBLAN"]:
        cur2.execute(f"SELECT COUNT(*) FROM {t}")
        print(f"     {t:<24} {cur2.fetchone()[0]:>8,} registros")
    conn2.close()
    print(f"\n✅  Concluído! [{ts.strftime('%H:%M:%S')}]\n")


if __name__ == "__main__":
    main()
