"""
services/ingestion_service.py
Orquestra a ingestão de dados brutos → tabelas RAW.
Cada método público corresponde a uma fonte de dados.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from src.config.settings import get_settings
from src.infra.db import get_session
from src.infra.http_clients.obrasgov_client import ObrasGovClient
from src.infra.http_clients.tcerj_client import TCERJClient
from src.infra.repositories import raw_repository as raw_repo

logger = logging.getLogger(__name__)
_settings = get_settings()


# ---------------------------------------------------------------------------
# ObrasGov – ingestão completa
# ---------------------------------------------------------------------------

def ingest_obrasgov() -> dict:
    """
    Ingestão completa do ObrasGov.br:
      1. Projetos (todo o RJ, filtro de Macaé fica na camada CLEAN)
      2. Execução física, financeira, contratos e geometria por projeto

    Retorna: {"projetos": int, "execucao_fisica": int, "contratos": int, "geometrias": int}
    """
    counters = {"projetos": 0, "execucao_fisica": 0, "contratos": 0, "geometrias": 0}

    with ObrasGovClient() as client:
        # --- Projetos ---
        logger.info("ObrasGov: iniciando ingestão de projetos…")
        all_ids: list[str] = []

        for page in client.get_projetos_investimento(uf=_settings.OBRASGOV_UF):
            # Diagnóstico: mostrar campos do primeiro projeto
            if not all_ids and page:
                sample = page[0]
                logger.info("ObrasGov: campos do projeto exemplo: %s", sorted(sample.keys()))

            with get_session() as session:
                for row in page:
                    raw_repo.upsert_projeto(session, row)
                    id_unico = row.get("idUnico")
                    if id_unico:
                        all_ids.append(id_unico)
                        counters["projetos"] += 1

        logger.info("ObrasGov: %d projetos ingeridos. Iniciando detalhes…", counters["projetos"])

        # --- Detalhes por projeto ---
        for id_unico in all_ids:
            try:
                # Execução física
                ef_list = client.get_execucao_fisica(id_unico)
                if ef_list:
                    with get_session() as session:
                        for ef in ef_list:
                            raw_repo.upsert_execucao_fisica(session, id_unico, ef)
                            counters["execucao_fisica"] += 1

                # Execução financeira (empenhos)
                fin_list = client.get_execucao_financeira(id_unico)
                if fin_list:
                    with get_session() as session:
                        for fin in fin_list:
                            raw_repo.upsert_execucao_financeira(session, id_unico, fin)

                # Contratos
                cont_list = client.get_contratos(id_unico)
                if cont_list:
                    with get_session() as session:
                        for cont in cont_list:
                            raw_repo.upsert_contrato(session, id_unico, cont)
                            counters["contratos"] += 1

                # Geometria
                geo_list = client.get_geometria(id_unico)
                if geo_list:
                    with get_session() as session:
                        for geo in geo_list:
                            raw_repo.insert_geometria(session, id_unico, geo)
                            counters["geometrias"] += 1

            except Exception as exc:
                logger.error("ObrasGov: erro ao processar detalhes de %s: %s", id_unico, exc)

    logger.info("ObrasGov ingestão concluída: %s", counters)
    return counters


# ---------------------------------------------------------------------------
# TCE-RJ – ingestão completa
# ---------------------------------------------------------------------------

def ingest_tcerj() -> dict:
    """
    Ingestão do TCE-RJ:
      1. /obras_tce (todas)
      2. /obras_paralisadas para anos configurados
    """
    counters = {"obras": 0, "paralisadas": 0}

    with TCERJClient() as client:
        # --- Obras TCE ---
        logger.info("TCE-RJ: iniciando ingestão de obras…")
        obras = client.get_obras()
        with get_session() as session:
            for obra in obras:
                raw_repo.insert_tcerj_obra(session, obra)
                counters["obras"] += 1
        logger.info("TCE-RJ: %d obras ingeridas", counters["obras"])

        # --- Obras Paralisadas ---
        for ano in _settings.TCERJ_ANOS_PARALISADAS:
            paralisadas = client.get_obras_paralisadas(ano)
            with get_session() as session:
                for p in paralisadas:
                    raw_repo.insert_tcerj_paralisada(session, p)
                    counters["paralisadas"] += 1
        logger.info("TCE-RJ: %d obras paralisadas ingeridas", counters["paralisadas"])

    return counters


# ---------------------------------------------------------------------------
# Convênios CSV – ingestão
# ---------------------------------------------------------------------------

def ingest_convenios_csv() -> dict:
    """
    Lê todos os arquivos CSV da pasta configurada e insere em raw.macae_convenios.
    """
    counters = {"arquivos": 0, "linhas": 0}
    pasta = Path(_settings.CONVENIOS_DIR)

    if not pasta.exists():
        logger.warning("Pasta de convênios não encontrada: %s", pasta)
        return counters

    csv_files = list(pasta.glob("*.csv"))
    if not csv_files:
        logger.warning("Nenhum CSV encontrado em %s", pasta)
        return counters

    for csv_path in csv_files:
        logger.info("Convênios: processando %s", csv_path.name)
        try:
            # SUPOSIÇÃO: encoding latin-1 comum em arquivos do governo brasileiro
            df = pd.read_csv(csv_path, encoding="latin-1", sep=";", quoting=3)
            counters["arquivos"] += 1

            with get_session() as session:
                for idx, row in df.iterrows():
                    raw_repo.insert_convenio(
                        session,
                        row.to_dict(),
                        arquivo=csv_path.name,
                        linha=int(idx) + 2,  # +2: cabeçalho = linha 1
                    )
                    counters["linhas"] += 1

        except Exception as exc:
            logger.error("Convênios: falha ao processar %s: %s", csv_path.name, exc)

    logger.info("Convênios: %d arquivos, %d linhas ingeridas", counters["arquivos"], counters["linhas"])
    return counters
