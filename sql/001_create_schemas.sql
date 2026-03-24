-- =============================================================================
-- GeoObras – DDL principal (Mês 1)
-- Esquemas: raw, clean, analytics + tabela de log ETL
--
-- NOTA SOBRE POSTGIS:
--   Se PostGIS estiver disponível, substitua os comentários marcados com
--   [POSTGIS] pelo tipo geometry(Point, 4326) e habilite a extensão:
--   CREATE EXTENSION IF NOT EXISTS postgis;
--   Enquanto não estiver disponível, os campos geometry ficam como TEXT (WKT).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- SCHEMAS
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- EXTENSÕES (descomente se PostGIS estiver disponível)
-- ---------------------------------------------------------------------------
-- CREATE EXTENSION IF NOT EXISTS postgis;
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- SCHEMA RAW
-- =============================================================================

-- -----------------------------------------------------------------------------
-- raw.obrasgov_projetos
-- Espelho de /projeto-investimento (ObrasGov.br)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.obrasgov_projetos (
    id_unico                  TEXT PRIMARY KEY,
    nome                      TEXT,
    cep                       TEXT,
    endereco                  TEXT,
    descricao                 TEXT,
    funcao_social             TEXT,
    meta_global               TEXT,
    data_inicial_prevista     DATE,
    data_final_prevista       DATE,
    data_inicial_efetiva      DATE,
    data_final_efetiva        DATE,
    data_cadastro             DATE,
    especie                   TEXT,
    natureza                  TEXT,
    situacao                  TEXT,
    uf                        TEXT,
    qtd_empregos_gerados      INTEGER,
    populacao_beneficiada     INTEGER,
    observacoes_pertinentes   TEXT,
    is_modelada_por_bim       BOOLEAN,
    -- Listas complexas (tomadores, executores, eixos, etc.) armazenadas como JSON
    tomadores                 JSONB,
    executores                JSONB,
    repassadores              JSONB,
    eixos                     JSONB,
    tipos                     JSONB,
    sub_tipos                 JSONB,
    fontes_de_recurso         JSONB,
    -- Payload completo para auditoria / reprocessamento
    payload_json              JSONB,
    ingestado_em              TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.obrasgov_execucao_fisica
-- Espelho de /execucao-fisica (ObrasGov.br)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.obrasgov_execucao_fisica (
    id_unico                     TEXT NOT NULL,
    data_situacao                DATE NOT NULL,
    percentual                   NUMERIC(5, 2),
    situacao                     TEXT,
    observacoes                  TEXT,
    em_operacao                  BOOLEAN,
    justificativa_em_operacao    TEXT,
    cancelamentos_paralisacoes   JSONB,
    documentos                   JSONB,
    payload_json                 JSONB,
    ingestado_em                 TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (id_unico, data_situacao)
);

-- -----------------------------------------------------------------------------
-- raw.obrasgov_execucao_financeira
-- Espelho de /execucao-financeira (empenhos)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.obrasgov_execucao_financeira (
    id_projeto_investimento   TEXT NOT NULL,
    nr_nota_empenho           TEXT NOT NULL,
    nome_esfera_orcamentaria  TEXT,
    nome_tipo_empenho         TEXT,
    fonte_recurso             TEXT,
    natureza_despesa          TEXT,
    numero_processo           TEXT,
    descricao_empenho         TEXT,
    valor_empenho             NUMERIC(18, 2),
    payload_json              JSONB,
    ingestado_em              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (id_projeto_investimento, nr_nota_empenho)
);

-- -----------------------------------------------------------------------------
-- raw.obrasgov_contratos
-- Espelho de /execucao-financeira/contrato
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.obrasgov_contratos (
    id_projeto_investimento   TEXT NOT NULL,
    numero_contrato           TEXT NOT NULL,
    vigencia_inicio           DATE,
    vigencia_fim              DATE,
    data_assinatura           DATE,
    data_publicacao           DATE,
    objeto                    TEXT,
    processo                  TEXT,
    valor_global              NUMERIC(18, 2),
    valor_acumulado           NUMERIC(18, 2),
    payload_json              JSONB,
    ingestado_em              TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (id_projeto_investimento, numero_contrato)
);

-- -----------------------------------------------------------------------------
-- raw.obrasgov_geometria
-- Espelho de /geometria (WKT)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.obrasgov_geometria (
    id                       SERIAL PRIMARY KEY,
    id_unico                 TEXT NOT NULL,
    geometria_wkt            TEXT,
    geometria_raw            TEXT,
    data_criacao             DATE,
    origem                   TEXT,
    data_metadado            DATE,
    info_adicionais          TEXT,
    nome_area_executora      TEXT,
    endereco_area_executora  TEXT,
    cep_area_executora       TEXT,
    pais_area_executora      TEXT,
    payload_json             JSONB,
    ingestado_em             TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_obrasgov_geometria_id_unico
    ON raw.obrasgov_geometria (id_unico);

-- -----------------------------------------------------------------------------
-- raw.tcerj_obras
-- Espelho de /obras_tce (TCE-RJ)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.tcerj_obras (
    id                      SERIAL PRIMARY KEY,
    objeto                  TEXT,
    empresa                 TEXT,
    data_inicio             DATE,
    previsao_conclusao      DATE,
    etapas                  TEXT,
    percentual_concluido    NUMERIC(5, 2),
    situacao                TEXT,
    contratados             NUMERIC(18, 2),
    praticados              NUMERIC(18, 2),
    registros_atualizados_ate DATE,
    motivo_paralisacao      TEXT,
    obra_paralisada         BOOLEAN,
    -- Campos adicionais preservados no payload completo
    payload_json            JSONB,
    ingestado_em            TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.tcerj_obras_paralisadas
-- Espelho de /obras_paralisadas (TCE-RJ)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.tcerj_obras_paralisadas (
    id                       SERIAL PRIMARY KEY,
    ano_paralisacao          INTEGER,
    data_paralisacao         DATE,
    tipo_ente                TEXT,
    ente                     TEXT,
    nome                     TEXT,
    funcao_governo           TEXT,
    numero_contrato          TEXT,
    cnpj_contratada          TEXT,
    nome_contratada          TEXT,
    valor_total_contrato     NUMERIC(18, 2),
    valor_pago_obra          NUMERIC(18, 2),
    tempo_paralisacao        TEXT,
    motivo_paralisacao       TEXT,
    data_inicio_obra         DATE,
    status_contrato          TEXT,
    classificacao_obra       TEXT,
    fonte_principal_recursos TEXT,
    payload_json             JSONB,
    ingestado_em             TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- raw.macae_convenios
-- CSV de Convênios/Parcerias de Macaé
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.macae_convenios (
    id_convenio        SERIAL PRIMARY KEY,
    numero_instrumento TEXT,
    unidade_gestora    TEXT,
    aditivo            TEXT,
    tipo_instrumento   TEXT,
    instituicao        TEXT,
    valor_concedente   NUMERIC(18, 2),
    valor_convenente   NUMERIC(18, 2),
    valor_total        NUMERIC(18, 2),
    arquivo_origem     TEXT,
    linha_origem       INTEGER,
    payload_json       JSONB,
    ingestado_em       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- SCHEMA CLEAN
-- =============================================================================

-- -----------------------------------------------------------------------------
-- clean.obras
-- Modelo unificado de obras (ObrasGov + TCE-RJ), filtrado para Macaé
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.obras (
    id_obra_geoobras         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    id_unico_obrasgov        TEXT,   -- FK lógica → raw.obrasgov_projetos
    id_obras_tce             INTEGER, -- FK lógica → raw.tcerj_obras (pode ser NULL)

    nome                     TEXT NOT NULL,
    descricao                TEXT,
    municipio                TEXT DEFAULT 'Macaé',
    uf                       TEXT DEFAULT 'RJ',
    codigo_municipio         INTEGER,   -- IBGE (3302403 = Macaé, RJ)
    bairro                   TEXT,
    logradouro               TEXT,

    -- SUPOSIÇÃO: status mapeado para enum textual; pode virar tipo ENUM depois
    status_obra              TEXT CHECK (status_obra IN (
                                 'planejada','em_execucao','concluida',
                                 'paralisada','cancelada','inacabada','desconhecida'
                             )),

    data_inicio              DATE,
    data_fim_prevista        DATE,
    data_fim_real            DATE,
    flag_data_fim_pendente   BOOLEAN DEFAULT FALSE,

    percentual_fisico        NUMERIC(5, 2),

    populacao_beneficiada    INTEGER,
    flag_populacao_suspeita  BOOLEAN DEFAULT FALSE,  -- TRUE se valor = 0

    empregos_gerados         INTEGER,
    flag_empregos_suspeitos  BOOLEAN DEFAULT FALSE,  -- TRUE se valor = 0

    valor_total_contratado   NUMERIC(18, 2),
    valor_pago_acumulado     NUMERIC(18, 2),
    valor_previsto_original  NUMERIC(18, 2),

    latitude                 NUMERIC(10, 7),
    longitude                NUMERIC(10, 7),
    -- [POSTGIS] Substitua TEXT por geometry(Point, 4326) se PostGIS disponível
    geom                     TEXT,  -- WKT, ex.: 'POINT(-41.78 -22.37)'

    fonte_principal          TEXT CHECK (fonte_principal IN ('obrasgov','tce','mista','convenio')),
    data_ultima_atualizacao  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_obras_status    ON clean.obras (status_obra);
CREATE INDEX IF NOT EXISTS idx_clean_obras_municipio ON clean.obras (municipio);
CREATE INDEX IF NOT EXISTS idx_clean_obras_obrasgov  ON clean.obras (id_unico_obrasgov);

-- -----------------------------------------------------------------------------
-- clean.contratos
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.contratos (
    id_contrato_geoobras     SERIAL PRIMARY KEY,
    id_contrato_obrasgov     TEXT,   -- numero_contrato de raw.obrasgov_contratos
    id_contrato_tce          TEXT,   -- número de contrato do TCE, se disponível
    numero_contrato          TEXT,
    unidade_gestora          TEXT,
    objeto                   TEXT,
    valor_contratado         NUMERIC(18, 2),
    valor_aditivos           NUMERIC(18, 2),
    valor_total_atualizado   NUMERIC(18, 2),
    data_inicio_vigencia     DATE,
    data_fim_vigencia        DATE,
    municipio                TEXT,
    codigo_municipio         INTEGER
);

-- -----------------------------------------------------------------------------
-- clean.obras_contratos  (N:M)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.obras_contratos (
    id_obra_geoobras     UUID NOT NULL,
    id_contrato_geoobras INTEGER NOT NULL,
    tipo_relacao         TEXT DEFAULT 'principal',
    PRIMARY KEY (id_obra_geoobras, id_contrato_geoobras)
);

-- -----------------------------------------------------------------------------
-- clean.convenios
-- Normalização de raw.macae_convenios
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean.convenios (
    id_convenio_geoobras SERIAL PRIMARY KEY,
    id_convenio_raw      INTEGER,  -- FK lógica → raw.macae_convenios
    numero_instrumento   TEXT,
    unidade_gestora      TEXT,
    aditivo              TEXT,
    tipo_instrumento     TEXT,
    instituicao          TEXT,
    valor_concedente     NUMERIC(18, 2),
    valor_convenente     NUMERIC(18, 2),
    valor_total          NUMERIC(18, 2),
    municipio            TEXT DEFAULT 'Macaé'
);

-- =============================================================================
-- SCHEMA ANALYTICS
-- =============================================================================

-- -----------------------------------------------------------------------------
-- analytics.metricas_obra
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.metricas_obra (
    id_obra_geoobras        UUID PRIMARY KEY,  -- FK lógica → clean.obras
    valor_total_contratado  NUMERIC(18, 2),
    valor_pago_acumulado    NUMERIC(18, 2),
    percentual_desembolso   NUMERIC(5, 2),
    percentual_fisico       NUMERIC(5, 2),
    data_inicio             DATE,
    data_fim_prevista       DATE,
    data_fim_real           DATE,
    dias_atraso             INTEGER,           -- NULL se não atrasada ou sem data_fim_prevista
    flag_possivel_atraso    BOOLEAN DEFAULT FALSE,
    calculado_em            TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- analytics.recorrencia_territorial
-- Estrutura base para Mês 2 (contagens por bairro/região)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.recorrencia_territorial (
    id_obra_geoobras UUID PRIMARY KEY,
    bairro           TEXT,
    -- [POSTGIS] Substitua TEXT por geometry(Point, 4326) se PostGIS disponível
    geom             TEXT   -- WKT
    -- Campos de contagem/agregação serão adicionados no Mês 2
);

-- =============================================================================
-- TABELA DE LOG ETL (schema público/padrão)
-- =============================================================================
CREATE TABLE IF NOT EXISTS etl_execucao (
    id             SERIAL PRIMARY KEY,
    data_execucao  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    fonte          TEXT CHECK (fonte IN ('obrasgov','tcerj','convenios','completa')),
    status         TEXT CHECK (status IN ('sucesso','erro_parcial','erro')),
    detalhes       JSONB
);
