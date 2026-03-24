"""
services/clean_service.py
Normaliza dados RAW → CLEAN.
Filtra para Macaé, faz matching heurístico ObrasGov ↔ TCE-RJ,
resolve datas pendentes, flags de qualidade e geometria.
"""

from __future__ import annotations

import logging
import re
import uuid
from datetime import date, datetime
from difflib import SequenceMatcher
from typing import Any, Optional

from src.config.settings import get_settings
from src.domain.enums import (
    OBRASGOV_STATUS_MAP,
    TCERJ_STATUS_MAP,
    FontePrincipal,
    StatusObra,
)
from src.infra.db import get_session
from src.infra.repositories import clean_repository as clean_repo
from src.infra.repositories.analytics_repository import upsert_recorrencia_territorial
from src.services.geometry_service import extract_lat_lon, wkt_to_geom_text

logger = logging.getLogger(__name__)
_settings = get_settings()

MUNICIPIO_ALVO = _settings.OBRASGOV_MUNICIPIO_ALVO.upper()

# Código IBGE de Macaé/RJ
COD_MUNICIPIO_MACAE = 3302403

# Marcadores textuais de "data pendente" no ObrasGov
PENDENTE_MARKERS = {"informacao pendente", "informação pendente", "pendente", "a definir", ""}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_macae(text_fields: list[str | None]) -> bool:
    """
    Verifica se algum dos campos de texto contém referência a Macaé.
    SUPOSIÇÃO: Usamos match de substring case-insensitive. Pode gerar falso
    positivo em nomes como "Nova Macaé" – aceitável no Mês 1.
    """
    for f in text_fields:
        if f and MUNICIPIO_ALVO in (f or "").upper():
            return True
    return False


def _parse_date(value: Any) -> Optional[date]:
    """Converte string de data (ISO ou BR) para date, retornando None se pendente."""
    if not value:
        return None
    s = str(value).strip()
    if s.lower() in PENDENTE_MARKERS:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s[:10], fmt[:8]).date()
        except ValueError:
            continue
    logger.debug("Não foi possível parsear data: %r", value)
    return None


def _is_date_pending(value: Any) -> bool:
    """Retorna True se o valor representa informação pendente."""
    if not value:
        return True
    return str(value).strip().lower() in PENDENTE_MARKERS


def _map_status_obrasgov(situacao: Optional[str]) -> str:
    if not situacao:
        return StatusObra.DESCONHECIDA.value
    return OBRASGOV_STATUS_MAP.get(situacao, StatusObra.DESCONHECIDA).value


def _map_status_tcerj(situacao: Optional[str], paralisada: bool = False) -> str:
    if paralisada:
        return StatusObra.PARALISADA.value
    if not situacao:
        return StatusObra.DESCONHECIDA.value
    return TCERJ_STATUS_MAP.get(situacao, StatusObra.DESCONHECIDA).value


def _similarity(a: str, b: str) -> float:
    """Similaridade de strings via SequenceMatcher (0–1)."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


# ---------------------------------------------------------------------------
# Normalização de obras ObrasGov
# ---------------------------------------------------------------------------

def _build_obra_from_obrasgov(
    proj: dict,
    ef_map: dict[str, dict],
    soma_empenhos: dict[str, float],
    contratos_map: dict[str, list[dict]],
    geo_map: dict[str, dict],
) -> dict:
    id_unico = proj["id_unico"]

    # Datas
    data_inicio = _parse_date(proj.get("data_inicial_efetiva") or proj.get("data_inicial_prevista"))
    data_fim_prevista = _parse_date(proj.get("data_final_prevista"))
    data_fim_real = _parse_date(proj.get("data_final_efetiva"))
    flag_data_fim = _is_date_pending(proj.get("data_final_efetiva")) and _is_date_pending(proj.get("data_final_prevista"))

    # Valores
    contratos = contratos_map.get(id_unico, [])
    valor_global = max((c.get("valor_global") or 0 for c in contratos), default=None)
    valor_acumulado = sum(c.get("valor_acumulado") or 0 for c in contratos) or None
    valor_pago = soma_empenhos.get(id_unico)

    # Execução física
    ef = ef_map.get(id_unico, {})
    pct_fisico = ef.get("percentual")

    # Geometria
    lat, lon, geom_wkt = None, None, None
    geo = geo_map.get(id_unico)
    if geo:
        wkt = geo.get("geometria_wkt") or geo.get("geometria_raw")
        lat, lon = extract_lat_lon(wkt)
        geom_wkt = wkt_to_geom_text(wkt)

    # Flags qualidade
    pop = proj.get("populacao_beneficiada")
    emp = proj.get("qtd_empregos_gerados")

    return {
        "id_obra_geoobras": str(uuid.uuid4()),
        "id_unico_obrasgov": id_unico,
        "id_obras_tce": None,
        "nome": proj.get("nome") or "Sem nome",
        "descricao": proj.get("descricao"),
        "municipio": MUNICIPIO_ALVO.title(),
        "uf": "RJ",
        "codigo_municipio": COD_MUNICIPIO_MACAE,
        "bairro": None,  # ObrasGov não tem campo de bairro direto
        "logradouro": proj.get("endereco"),
        "status_obra": _map_status_obrasgov(proj.get("situacao")),
        "data_inicio": data_inicio,
        "data_fim_prevista": data_fim_prevista,
        "data_fim_real": data_fim_real,
        "flag_data_fim_pendente": flag_data_fim,
        "percentual_fisico": pct_fisico,
        "populacao_beneficiada": pop,
        "flag_populacao_suspeita": pop is not None and pop == 0,
        "empregos_gerados": emp,
        "flag_empregos_suspeitos": emp is not None and emp == 0,
        "valor_total_contratado": valor_global,
        "valor_pago_acumulado": valor_pago,
        "valor_previsto_original": None,  # ObrasGov não tem campo direto
        "latitude": lat,
        "longitude": lon,
        "geom": geom_wkt,
        "fonte_principal": FontePrincipal.OBRASGOV.value,
        # Manter contratos para posterior inserção em clean.contratos
        "_contratos_raw": contratos,
    }


# ---------------------------------------------------------------------------
# Normalização de obras TCE-RJ
# ---------------------------------------------------------------------------

def _build_obra_from_tcerj(row: dict) -> dict:
    data_inicio = _parse_date(row.get("data_inicio"))
    data_fim_prevista = _parse_date(row.get("previsao_conclusao"))

    return {
        "id_obra_geoobras": str(uuid.uuid4()),
        "id_unico_obrasgov": None,
        "id_obras_tce": row.get("id"),
        "nome": row.get("objeto") or "Sem nome (TCE)",
        "descricao": None,
        "municipio": MUNICIPIO_ALVO.title(),
        "uf": "RJ",
        "codigo_municipio": COD_MUNICIPIO_MACAE,
        "bairro": None,
        "logradouro": None,
        "status_obra": _map_status_tcerj(row.get("situacao"), bool(row.get("obra_paralisada"))),
        "data_inicio": data_inicio,
        "data_fim_prevista": data_fim_prevista,
        "data_fim_real": None,
        "flag_data_fim_pendente": data_fim_prevista is None,
        "percentual_fisico": row.get("percentual_concluido"),
        "populacao_beneficiada": None,
        "flag_populacao_suspeita": False,
        "empregos_gerados": None,
        "flag_empregos_suspeitos": False,
        "valor_total_contratado": row.get("contratados"),
        "valor_pago_acumulado": row.get("praticados"),
        "valor_previsto_original": None,
        "latitude": None,
        "longitude": None,
        "geom": None,
        "fonte_principal": FontePrincipal.TCE.value,
        "_contratos_raw": [],
    }


def _build_obra_from_tcerj_paralisada(row: dict) -> dict:
    """Normaliza uma obra paralisada do TCE-RJ (raw.tcerj_obras_paralisadas)."""
    data_inicio = _parse_date(row.get("data_inicio_obra"))

    return {
        "id_obra_geoobras": str(uuid.uuid4()),
        "id_unico_obrasgov": None,
        "id_obras_tce": row.get("id"),
        "nome": row.get("nome") or "Sem nome (TCE paralisada)",
        "descricao": row.get("funcao_governo"),
        "municipio": (row.get("ente") or MUNICIPIO_ALVO).strip().title(),
        "uf": "RJ",
        "codigo_municipio": COD_MUNICIPIO_MACAE,
        "bairro": None,
        "logradouro": None,
        "status_obra": StatusObra.PARALISADA.value,
        "data_inicio": data_inicio,
        "data_fim_prevista": None,
        "data_fim_real": None,
        "flag_data_fim_pendente": True,
        "percentual_fisico": None,
        "populacao_beneficiada": None,
        "flag_populacao_suspeita": False,
        "empregos_gerados": None,
        "flag_empregos_suspeitos": False,
        "valor_total_contratado": row.get("valor_total_contrato"),
        "valor_pago_acumulado": row.get("valor_pago_obra"),
        "valor_previsto_original": None,
        "latitude": None,
        "longitude": None,
        "geom": None,
        "fonte_principal": FontePrincipal.TCE.value,
        "_contratos_raw": [],
    }


# ---------------------------------------------------------------------------
# Matching ObrasGov ↔ TCE-RJ
# HEURÍSTICA INICIAL: similaridade de nome ≥ 0.6
# Pode ser refinado com NLP ou outros campos (contrato, CNPJ) no Mês 2+
# ---------------------------------------------------------------------------

SIMILARITY_THRESHOLD = 0.60


def _match_obrasgov_com_tcerj(
    obras_gov: list[dict],
    obras_tce: list[dict],
) -> list[dict]:
    """
    Para cada obra TCE sem match, tenta encontrar correspondente ObrasGov
    pelo nome. Quando há match, associa id_obras_tce e marca fonte como 'mista'.
    Obras TCE sem match são incluídas como novas entradas.
    """
    matched_tce_ids: set[int] = set()
    result = list(obras_gov)  # começa com todas as obras ObrasGov

    for tce in obras_tce:
        nome_tce = (tce.get("nome") or "").lower()
        melhor_score = 0.0
        melhor_gov = None

        for gov in result:
            nome_gov = (gov.get("nome") or "").lower()
            score = _similarity(nome_tce, nome_gov)
            if score > melhor_score:
                melhor_score = score
                melhor_gov = gov

        if melhor_gov and melhor_score >= SIMILARITY_THRESHOLD:
            # associa
            melhor_gov["id_obras_tce"] = tce.get("id_obras_tce")
            melhor_gov["fonte_principal"] = FontePrincipal.MISTA.value
            # prefere % físico TCE se ObrasGov não tiver
            if melhor_gov.get("percentual_fisico") is None:
                melhor_gov["percentual_fisico"] = tce.get("percentual_fisico")
            matched_tce_ids.add(tce.get("id_obras_tce"))
        else:
            # TCE sem match → nova obra
            result.append(tce)

    return result


# ---------------------------------------------------------------------------
# Pipeline principal CLEAN
# ---------------------------------------------------------------------------

def run_clean() -> dict:
    """
    Executa a camada CLEAN:
    1. Lê RAW
    2. Filtra Macaé
    3. Normaliza e faz matching
    4. Grava em clean.obras + clean.contratos + clean.obras_contratos
    5. Normaliza convênios
    """
    counters = {"obras": 0, "contratos": 0, "convenios": 0}

    with get_session() as session:
        logger.info("CLEAN: carregando dados RAW…")
        projetos = clean_repo.fetch_all_projetos_obrasgov(session)
        ef_map = clean_repo.fetch_execucao_fisica_latest(session)
        soma_empenhos = clean_repo.fetch_soma_empenhos(session)
        contratos_map = clean_repo.fetch_contratos_obrasgov(session)
        geo_map = clean_repo.fetch_geometria_by_id_unico(session)
        tcerj_obras = clean_repo.fetch_all_tcerj_obras(session)
        tcerj_paralisadas = clean_repo.fetch_all_tcerj_paralisadas_macae(session)
        raw_convenios_sql = "SELECT * FROM raw.macae_convenios"
        from sqlalchemy import text
        raw_convenios = [dict(r) for r in session.execute(text(raw_convenios_sql)).mappings().all()]

    logger.info(
        "CLEAN: %d projetos ObrasGov, %d obras TCE, %d paralisadas TCE (Macaé)",
        len(projetos), len(tcerj_obras), len(tcerj_paralisadas),
    )

    # --- Filtrar Macaé (ObrasGov) ---
    macae_gov = [
        p for p in projetos
        if _is_macae([p.get("endereco"), p.get("nome"), p.get("descricao"), p.get("municipio")])
    ]
    logger.info("CLEAN: %d projetos ObrasGov após filtro Macaé", len(macae_gov))

    # --- Normalizar ObrasGov ---
    obras_gov_norm = [
        _build_obra_from_obrasgov(p, ef_map, soma_empenhos, contratos_map, geo_map)
        for p in macae_gov
    ]

    # --- Normalizar TCE obras (internas do TCE-RJ, sem filtro de município) ---
    obras_tce_norm = [_build_obra_from_tcerj(r) for r in tcerj_obras]

    # --- Normalizar TCE paralisadas de Macaé (já filtradas por ente) ---
    obras_paralisadas_norm = [_build_obra_from_tcerj_paralisada(r) for r in tcerj_paralisadas]

    todas_tce = obras_tce_norm + obras_paralisadas_norm

    # --- Matching ---
    obras_finais = _match_obrasgov_com_tcerj(obras_gov_norm, todas_tce)
    logger.info("CLEAN: %d obras após matching", len(obras_finais))

    # --- Gravar clean.obras + contratos ---
    with get_session() as session:
        for obra in obras_finais:
            contratos_raw = obra.pop("_contratos_raw", [])
            clean_repo.upsert_obra_clean(session, obra)
            counters["obras"] += 1

            for cont in contratos_raw:
                try:
                    id_cont = clean_repo.insert_contrato_clean(session, obra.get("id_unico_obrasgov", ""), cont)
                    clean_repo.link_obra_contrato(session, obra["id_obra_geoobras"], id_cont)
                    counters["contratos"] += 1
                except Exception as exc:
                    logger.warning("CLEAN: falha ao inserir contrato %s: %s", cont.get("numero_contrato"), exc)

    # --- Normalizar convênios ---
    with get_session() as session:
        for conv in raw_convenios:
            try:
                clean_repo.insert_convenio_clean(session, conv)
                counters["convenios"] += 1
            except Exception as exc:
                logger.warning("CLEAN: falha ao inserir convênio %s: %s", conv.get("numero_instrumento"), exc)

    logger.info("CLEAN concluído: %s", counters)
    return counters
