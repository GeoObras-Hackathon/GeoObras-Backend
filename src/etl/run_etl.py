"""
etl/run_etl.py
Script principal do pipeline ETL. Pode ser chamado diretamente ou via cron.
Fluxo: RAW (ingestão) → CLEAN (normalização) → Analytics (métricas)

Uso:
    python -m src.etl.run_etl
    python -m src.etl.run_etl --fonte obrasgov   # só uma fonte
    python -m src.etl.run_etl --skip-raw          # pula ingestão, reprocessa CLEAN
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime

# Configura logging antes de qualquer import de serviço
import src.logging_config as _lc
_lc.setup_logging()

from src.infra.db import get_session, test_connection
from src.infra.repositories.analytics_repository import insert_etl_log
from src.services import analytics_service, ingestion_service
from src.services.clean_service import run_clean

logger = logging.getLogger("etl.run_etl")


def run_full_etl(skip_raw: bool = False, fonte: str | None = None) -> None:
    """
    Executa o ETL completo ou parcial.

    Args:
        skip_raw: Se True, pula a fase de ingestão (RAW).
        fonte: Se fornecido, executa apenas a ingestão desta fonte
               ('obrasgov', 'tcerj', 'convenios').
    """
    inicio = datetime.now()
    logger.info("=" * 60)
    logger.info("GeoObras ETL – início: %s", inicio.isoformat())
    logger.info("=" * 60)

    # Verifica conexão com o banco
    if not test_connection():
        logger.error("Não foi possível conectar ao banco de dados. Abortando.")
        sys.exit(1)

    erros: list[str] = []
    counters: dict = {}

    # ------------------------------------------------------------------
    # Fase 1: Ingestão RAW
    # ------------------------------------------------------------------
    if not skip_raw:
        executar_obrasgov = fonte in (None, "obrasgov")
        executar_tcerj = fonte in (None, "tcerj")
        executar_convenios = fonte in (None, "convenios")

        if executar_obrasgov:
            logger.info(">>> FASE 1a: Ingestão ObrasGov…")
            try:
                c = ingestion_service.ingest_obrasgov()
                counters["obrasgov"] = c
                _log_etl("obrasgov", "sucesso", c)
            except Exception as exc:
                logger.error("Ingestão ObrasGov falhou: %s", exc, exc_info=True)
                erros.append(f"obrasgov: {exc}")
                _log_etl("obrasgov", "erro", {"erro": str(exc)})

        if executar_tcerj:
            logger.info(">>> FASE 1b: Ingestão TCE-RJ…")
            try:
                c = ingestion_service.ingest_tcerj()
                counters["tcerj"] = c
                _log_etl("tcerj", "sucesso", c)
            except Exception as exc:
                logger.error("Ingestão TCE-RJ falhou: %s", exc, exc_info=True)
                erros.append(f"tcerj: {exc}")
                _log_etl("tcerj", "erro", {"erro": str(exc)})

        if executar_convenios:
            logger.info(">>> FASE 1c: Ingestão Convênios CSV…")
            try:
                c = ingestion_service.ingest_convenios_csv()
                counters["convenios"] = c
                _log_etl("convenios", "sucesso", c)
            except Exception as exc:
                logger.error("Ingestão Convênios falhou: %s", exc, exc_info=True)
                erros.append(f"convenios: {exc}")
                _log_etl("convenios", "erro", {"erro": str(exc)})
    else:
        logger.info(">>> FASE 1: Ingestão RAW PULADA (--skip-raw)")

    # ------------------------------------------------------------------
    # Fase 2: Normalização CLEAN
    # Sempre executa após RAW — --fonte apenas filtra qual fonte ingerir,
    # não deve pular o reprocessamento das camadas seguintes.
    # ------------------------------------------------------------------
    logger.info(">>> FASE 2: Normalização CLEAN…")
    try:
        c = run_clean()
        counters["clean"] = c
    except Exception as exc:
        logger.error("Camada CLEAN falhou: %s", exc, exc_info=True)
        erros.append(f"clean: {exc}")

    # ------------------------------------------------------------------
    # Fase 3: Analytics
    # ------------------------------------------------------------------
    logger.info(">>> FASE 3: Cálculo de Analytics…")
    try:
        c = analytics_service.run_analytics()
        counters["analytics"] = c
    except Exception as exc:
        logger.error("Analytics falhou: %s", exc, exc_info=True)
        erros.append(f"analytics: {exc}")

    # ------------------------------------------------------------------
    # Log final
    # ------------------------------------------------------------------
    fim = datetime.now()
    duracao = (fim - inicio).total_seconds()
    status_final = "erro_parcial" if erros else "sucesso"
    if erros and not counters:
        status_final = "erro"

    detalhes = {
        "duracao_segundos": duracao,
        "counters": counters,
        "erros": erros,
    }
    _log_etl("completa", status_final, detalhes)

    logger.info("=" * 60)
    logger.info("ETL finalizado em %.1fs – status: %s", duracao, status_final)
    if erros:
        logger.warning("Erros encontrados: %s", erros)
    logger.info("=" * 60)


def _log_etl(fonte: str, status: str, detalhes: dict) -> None:
    try:
        with get_session() as session:
            insert_etl_log(session, fonte=fonte, status=status, detalhes=detalhes)
    except Exception as exc:
        logger.error("Falha ao registrar log ETL: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="GeoObras ETL Runner")
    parser.add_argument(
        "--fonte",
        choices=["obrasgov", "tcerj", "convenios"],
        help="Executa apenas a ingestão de uma fonte específica",
    )
    parser.add_argument(
        "--skip-raw",
        action="store_true",
        help="Pula a fase de ingestão RAW e reprocessa apenas CLEAN + Analytics",
    )
    args = parser.parse_args()
    run_full_etl(skip_raw=args.skip_raw, fonte=args.fonte)


if __name__ == "__main__":
    main()
