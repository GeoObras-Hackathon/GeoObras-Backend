"""
infra/repositories/raw_repository.py
Operações de escrita (upsert/insert) nas tabelas do esquema RAW.
Usa SQLAlchemy Core para queries explícitas (sem ORM pesado).
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _clean_nan(obj: Any) -> Any:
    """Substitui float NaN/Inf por None recursivamente (pandas usa NaN para células vazias)."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_nan(v) for v in obj]
    return obj


def _jsonb(obj: Any) -> str | None:
    """Serializa para string JSON, aceito pelo psycopg2 em campos JSONB."""
    if obj is None:
        return None
    return json.dumps(_clean_nan(obj), ensure_ascii=False, default=str)


def _int(v: Any) -> int | None:
    """Converte valor para int, retornando None para strings vazias ou inválidas."""
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _ts_ms_to_date(v: Any) -> str | None:
    """Converte timestamp em milissegundos (int) para string ISO 'YYYY-MM-DD'."""
    if v is None or v == "":
        return None
    try:
        from datetime import datetime, timezone
        ts = int(v) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return None


def _date_br(v: Any) -> str | None:
    """Converte 'DD/MM/YYYY' → 'YYYY-MM-DD'; retorna None se inválido."""
    if v is None or v == "":
        return None
    try:
        from datetime import datetime
        return datetime.strptime(str(v).strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _pct(v: Any) -> float | None:
    """Converte '41,10%' ou '100%' → float; retorna None se inválido."""
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace("%", "").replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


def _bool_sim_nao(v: Any) -> bool | None:
    """Converte 'SIM'/'NÃO' (string) → bool."""
    if v is None:
        return None
    s = str(v).strip().upper()
    if s in ("SIM", "S", "TRUE", "1", "YES"):
        return True
    if s in ("NÃO", "NAO", "N", "FALSE", "0", "NO"):
        return False
    return None


# ---------------------------------------------------------------------------
# ObrasGov – Projetos
# ---------------------------------------------------------------------------

UPSERT_PROJETO = text("""
    INSERT INTO raw.obrasgov_projetos (
        id_unico, nome, cep, endereco, descricao, funcao_social, meta_global,
        data_inicial_prevista, data_final_prevista,
        data_inicial_efetiva, data_final_efetiva,
        data_cadastro, especie, natureza, situacao, uf,
        qtd_empregos_gerados, populacao_beneficiada,
        observacoes_pertinentes, is_modelada_por_bim,
        tomadores, executores, repassadores, eixos,
        tipos, sub_tipos, fontes_de_recurso, payload_json
    ) VALUES (
        :id_unico, :nome, :cep, :endereco, :descricao, :funcao_social, :meta_global,
        :data_inicial_prevista, :data_final_prevista,
        :data_inicial_efetiva, :data_final_efetiva,
        :data_cadastro, :especie, :natureza, :situacao, :uf,
        :qtd_empregos_gerados, :populacao_beneficiada,
        :observacoes_pertinentes, :is_modelada_por_bim,
        CAST(:tomadores AS jsonb), CAST(:executores AS jsonb), CAST(:repassadores AS jsonb), CAST(:eixos AS jsonb),
        CAST(:tipos AS jsonb), CAST(:sub_tipos AS jsonb), CAST(:fontes_de_recurso AS jsonb), CAST(:payload_json AS jsonb)
    )
    ON CONFLICT (id_unico) DO UPDATE SET
        nome = EXCLUDED.nome,
        situacao = EXCLUDED.situacao,
        data_final_efetiva = EXCLUDED.data_final_efetiva,
        qtd_empregos_gerados = EXCLUDED.qtd_empregos_gerados,
        populacao_beneficiada = EXCLUDED.populacao_beneficiada,
        payload_json = EXCLUDED.payload_json,
        ingestado_em = NOW()
""")


def upsert_projeto(session: Session, row: dict[str, Any]) -> None:
    session.execute(UPSERT_PROJETO, {
        "id_unico": row.get("idUnico"),
        "nome": row.get("nome"),
        "cep": row.get("cep"),
        "endereco": row.get("endereco"),
        "descricao": row.get("descricao"),
        "funcao_social": row.get("funcaoSocial"),
        "meta_global": row.get("metaGlobal"),
        "data_inicial_prevista": row.get("dataInicialPrevista"),
        "data_final_prevista": row.get("dataFinalPrevista"),
        "data_inicial_efetiva": row.get("dataInicialEfetiva"),
        "data_final_efetiva": row.get("dataFinalEfetiva"),
        "data_cadastro": row.get("dataCadastro"),
        "especie": row.get("especie"),
        "natureza": row.get("natureza"),
        "situacao": row.get("situacao"),
        "uf": row.get("uf"),
        "qtd_empregos_gerados": _int(row.get("qdtEmpregosGerados")),
        "populacao_beneficiada": _int(row.get("populacaoBeneficiada")),
        "observacoes_pertinentes": row.get("observacoesPertinentes"),
        "is_modelada_por_bim": row.get("isModeladaPorBim"),
        "tomadores": _jsonb(row.get("tomadores")),
        "executores": _jsonb(row.get("executores")),
        "repassadores": _jsonb(row.get("repassadores")),
        "eixos": _jsonb(row.get("eixos")),
        "tipos": _jsonb(row.get("tipos")),
        "sub_tipos": _jsonb(row.get("subTipos")),
        "fontes_de_recurso": _jsonb(row.get("fontesDeRecurso")),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# ObrasGov – Execução Física
# ---------------------------------------------------------------------------

UPSERT_EF = text("""
    INSERT INTO raw.obrasgov_execucao_fisica (
        id_unico, data_situacao, percentual, situacao, observacoes,
        em_operacao, justificativa_em_operacao,
        cancelamentos_paralisacoes, documentos, payload_json
    ) VALUES (
        :id_unico, :data_situacao, :percentual, :situacao, :observacoes,
        :em_operacao, :justificativa_em_operacao,
        CAST(:cancelamentos_paralisacoes AS jsonb), CAST(:documentos AS jsonb), CAST(:payload_json AS jsonb)
    )
    ON CONFLICT (id_unico, data_situacao) DO UPDATE SET
        percentual = EXCLUDED.percentual,
        situacao   = EXCLUDED.situacao,
        payload_json = EXCLUDED.payload_json,
        ingestado_em = NOW()
""")


def upsert_execucao_fisica(session: Session, id_unico: str, row: dict[str, Any]) -> None:
    data_sit = row.get("dataSituacao") or row.get("data_situacao")
    if not data_sit:
        logger.warning("execucao_fisica sem data_situacao para %s – ignorado", id_unico)
        return
    session.execute(UPSERT_EF, {
        "id_unico": id_unico,
        "data_situacao": data_sit,
        "percentual": row.get("percentual"),
        "situacao": row.get("situacao"),
        "observacoes": row.get("observacoes"),
        "em_operacao": row.get("emOperacao"),
        "justificativa_em_operacao": row.get("justificativaEmOperacao"),
        "cancelamentos_paralisacoes": _jsonb(row.get("cancelamentosParalisacoes")),
        "documentos": _jsonb(row.get("documentos")),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# ObrasGov – Execução Financeira (empenhos)
# ---------------------------------------------------------------------------

UPSERT_FINANCEIRA = text("""
    INSERT INTO raw.obrasgov_execucao_financeira (
        id_projeto_investimento, nr_nota_empenho,
        nome_esfera_orcamentaria, nome_tipo_empenho,
        fonte_recurso, natureza_despesa, numero_processo,
        descricao_empenho, valor_empenho, payload_json
    ) VALUES (
        :id_projeto, :nr_nota,
        :nome_esfera, :nome_tipo,
        :fonte_recurso, :natureza_despesa, :numero_processo,
        :descricao_empenho, :valor_empenho, CAST(:payload_json AS jsonb)
    )
    ON CONFLICT (id_projeto_investimento, nr_nota_empenho) DO UPDATE SET
        valor_empenho = EXCLUDED.valor_empenho,
        payload_json  = EXCLUDED.payload_json,
        ingestado_em  = NOW()
""")


def upsert_execucao_financeira(session: Session, id_projeto: str, row: dict[str, Any]) -> None:
    nr = row.get("nrNotaEmpenho") or row.get("nr_nota_empenho")
    if not nr:
        logger.warning("execucao_financeira sem nrNotaEmpenho para %s – ignorado", id_projeto)
        return
    session.execute(UPSERT_FINANCEIRA, {
        "id_projeto": id_projeto,
        "nr_nota": nr,
        "nome_esfera": row.get("nomeEsferaOrcamentaria"),
        "nome_tipo": row.get("nomeTipoEmpenho"),
        "fonte_recurso": row.get("fonteRecurso"),
        "natureza_despesa": row.get("naturezaDespesa"),
        "numero_processo": row.get("numeroProcesso"),
        "descricao_empenho": row.get("descricaoEmpenho"),
        "valor_empenho": row.get("valorEmpenho"),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# ObrasGov – Contratos
# ---------------------------------------------------------------------------

UPSERT_CONTRATO = text("""
    INSERT INTO raw.obrasgov_contratos (
        id_projeto_investimento, numero_contrato,
        vigencia_inicio, vigencia_fim, data_assinatura, data_publicacao,
        objeto, processo, valor_global, valor_acumulado, payload_json
    ) VALUES (
        :id_projeto, :numero_contrato,
        :vigencia_inicio, :vigencia_fim, :data_assinatura, :data_publicacao,
        :objeto, :processo, :valor_global, :valor_acumulado, CAST(:payload_json AS jsonb)
    )
    ON CONFLICT (id_projeto_investimento, numero_contrato) DO UPDATE SET
        valor_global     = EXCLUDED.valor_global,
        valor_acumulado  = EXCLUDED.valor_acumulado,
        vigencia_fim     = EXCLUDED.vigencia_fim,
        payload_json     = EXCLUDED.payload_json,
        ingestado_em     = NOW()
""")


def upsert_contrato(session: Session, id_projeto: str, row: dict[str, Any]) -> None:
    numero = row.get("numeroContrato") or row.get("numero_contrato")
    if not numero:
        logger.warning("contrato sem numeroContrato para %s – ignorado", id_projeto)
        return
    session.execute(UPSERT_CONTRATO, {
        "id_projeto": id_projeto,
        "numero_contrato": numero,
        "vigencia_inicio": row.get("vigenciaInicio"),
        "vigencia_fim": row.get("vigenciaFim"),
        "data_assinatura": row.get("dataAssinatura"),
        "data_publicacao": row.get("dataPublicacao"),
        "objeto": row.get("objeto"),
        "processo": row.get("processo"),
        "valor_global": row.get("valorGlobal"),
        "valor_acumulado": row.get("valorAcumulado"),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# ObrasGov – Geometria
# ---------------------------------------------------------------------------

INSERT_GEOMETRIA = text("""
    INSERT INTO raw.obrasgov_geometria (
        id_unico, geometria_wkt, geometria_raw, data_criacao, origem,
        data_metadado, info_adicionais, nome_area_executora,
        endereco_area_executora, cep_area_executora, pais_area_executora,
        payload_json
    ) VALUES (
        :id_unico, :geometria_wkt, :geometria_raw, :data_criacao, :origem,
        :data_metadado, :info_adicionais, :nome_area_executora,
        :endereco_area_executora, :cep_area_executora, :pais_area_executora,
        CAST(:payload_json AS jsonb)
    )
""")


def insert_geometria(session: Session, id_unico: str, row: dict[str, Any]) -> None:
    session.execute(INSERT_GEOMETRIA, {
        "id_unico": id_unico,
        "geometria_wkt": row.get("geometriaWkt"),
        "geometria_raw": row.get("geometria"),
        "data_criacao": row.get("dataCriacao"),
        "origem": row.get("origem"),
        "data_metadado": row.get("dataMetadado"),
        "info_adicionais": row.get("infoAdicionais"),
        "nome_area_executora": row.get("nomeAreaExecutora"),
        "endereco_area_executora": row.get("enderecoAreaExecutora"),
        "cep_area_executora": row.get("cepAreaExecutora"),
        "pais_area_executora": row.get("paisAreaExecutora"),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# TCE-RJ – Obras
# ---------------------------------------------------------------------------

INSERT_TCERJ_OBRA = text("""
    INSERT INTO raw.tcerj_obras (
        objeto, empresa, data_inicio, previsao_conclusao, etapas,
        percentual_concluido, situacao, contratados, praticados,
        registros_atualizados_ate, motivo_paralisacao, obra_paralisada,
        payload_json
    ) VALUES (
        :objeto, :empresa, :data_inicio, :previsao_conclusao, :etapas,
        :percentual_concluido, :situacao, :contratados, :praticados,
        :registros_atualizados_ate, :motivo_paralisacao, :obra_paralisada,
        CAST(:payload_json AS jsonb)
    )
    RETURNING id
""")


def insert_tcerj_obra(session: Session, row: dict[str, Any]) -> int:
    # Contratados/Praticados são URLs para xlsx (não valores numéricos) → armazena None
    result = session.execute(INSERT_TCERJ_OBRA, {
        "objeto": row.get("Objeto"),
        "empresa": row.get("Empresa"),
        "data_inicio": _ts_ms_to_date(row.get("DataInicio")),
        "previsao_conclusao": _date_br(row.get("PrevisaoConclusao")),
        "etapas": row.get("Etapas"),
        "percentual_concluido": _pct(row.get("PercentualConcluido")),
        "situacao": row.get("Situacao"),
        "contratados": None,   # API retorna URL para xlsx, não valor monetário
        "praticados": None,    # idem
        "registros_atualizados_ate": _ts_ms_to_date(row.get("RegistrosAtualizadosAte")),
        "motivo_paralisacao": row.get("MotivoParalisacao"),
        "obra_paralisada": _bool_sim_nao(row.get("ObraParalisada")),
        "payload_json": _jsonb(row),
    })
    return result.scalar()


# ---------------------------------------------------------------------------
# TCE-RJ – Obras Paralisadas
# ---------------------------------------------------------------------------

INSERT_TCERJ_PARALISADA = text("""
    INSERT INTO raw.tcerj_obras_paralisadas (
        ano_paralisacao, data_paralisacao, tipo_ente, ente, nome,
        funcao_governo, numero_contrato, cnpj_contratada, nome_contratada,
        valor_total_contrato, valor_pago_obra, tempo_paralisacao,
        motivo_paralisacao, data_inicio_obra, status_contrato,
        classificacao_obra, fonte_principal_recursos, payload_json
    ) VALUES (
        :ano_paralisacao, :data_paralisacao, :tipo_ente, :ente, :nome,
        :funcao_governo, :numero_contrato, :cnpj_contratada, :nome_contratada,
        :valor_total_contrato, :valor_pago_obra, :tempo_paralisacao,
        :motivo_paralisacao, :data_inicio_obra, :status_contrato,
        :classificacao_obra, :fonte_principal_recursos, CAST(:payload_json AS jsonb)
    )
""")


def insert_tcerj_paralisada(session: Session, row: dict[str, Any]) -> None:
    session.execute(INSERT_TCERJ_PARALISADA, {
        "ano_paralisacao": _int(row.get("AnoParalisacao")),
        "data_paralisacao": _ts_ms_to_date(row.get("DataParalisacao")),
        "tipo_ente": row.get("TipoEnte"),
        "ente": row.get("Ente"),
        "nome": row.get("Nome"),
        "funcao_governo": row.get("FuncaoGoverno"),
        "numero_contrato": row.get("NumeroContrato"),
        "cnpj_contratada": row.get("CNPJContratada"),
        "nome_contratada": row.get("NomeContratada"),
        "valor_total_contrato": row.get("ValorTotalContrato"),
        "valor_pago_obra": row.get("ValorPagoObra"),
        "tempo_paralisacao": row.get("TempoParalizacao"),
        "motivo_paralisacao": row.get("MotivoParalisacao"),
        "data_inicio_obra": _ts_ms_to_date(row.get("DataInicioObra")),
        "status_contrato": row.get("StatusContrato"),
        "classificacao_obra": row.get("ClassificacaoObra"),
        "fonte_principal_recursos": row.get("FontePrincipalRecursos"),
        "payload_json": _jsonb(row),
    })


# ---------------------------------------------------------------------------
# Macaé – Convênios (CSV)
# ---------------------------------------------------------------------------

INSERT_CONVENIO = text("""
    INSERT INTO raw.macae_convenios (
        numero_instrumento, unidade_gestora, aditivo, tipo_instrumento,
        instituicao, valor_concedente, valor_convenente, valor_total,
        arquivo_origem, linha_origem, payload_json
    ) VALUES (
        :numero_instrumento, :unidade_gestora, :aditivo, :tipo_instrumento,
        :instituicao, :valor_concedente, :valor_convenente, :valor_total,
        :arquivo_origem, :linha_origem, CAST(:payload_json AS jsonb)
    )
""")


def insert_convenio(session: Session, row: dict[str, Any], arquivo: str, linha: int) -> None:
    def _money(v: Any) -> float | None:
        if v is None:
            return None
        try:
            return float(str(v).replace(".", "").replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    session.execute(INSERT_CONVENIO, {
        "numero_instrumento": row.get("Nº Instrumento"),
        "unidade_gestora": row.get("Unidade Gestora"),
        "aditivo": row.get("Aditivo"),
        "tipo_instrumento": row.get("Tipo de Instrumento"),
        "instituicao": row.get("Instituição"),
        "valor_concedente": _money(row.get("Valor Concedente")),
        "valor_convenente": _money(row.get("Valor Convenente")),
        "valor_total": _money(row.get("Valor Total")),
        "arquivo_origem": arquivo,
        "linha_origem": linha,
        "payload_json": _jsonb(row),
    })
