"""
services/analytics_service.py
Calcula métricas simples a partir das tabelas CLEAN e preenche analytics.*.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from src.infra.db import get_session
from src.infra.repositories.analytics_repository import (
    fetch_obras_para_analytics,
    upsert_metrica,
    upsert_recorrencia_territorial,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cálculos
# ---------------------------------------------------------------------------

def _calc_pct_desembolso(valor_contratado: float | None, valor_pago: float | None) -> float | None:
    if valor_contratado and valor_contratado > 0 and valor_pago is not None:
        return round(valor_pago / valor_contratado * 100, 2)
    return None


def _calc_dias_atraso(
    data_fim_prevista: date | None,
    data_fim_real: date | None,
    status: str | None,
) -> tuple[int | None, bool]:
    """
    Retorna (dias_atraso, flag_possivel_atraso).
    - dias_atraso > 0 → atrasada (concluída tardiamente ou ainda em execução além do prazo)
    - flag_possivel_atraso → True se atrasada
    """
    hoje = date.today()

    if not data_fim_prevista:
        return None, False

    if data_fim_real:
        # obra concluída: compara data real com prevista
        delta = (data_fim_real - data_fim_prevista).days
        return max(delta, 0) or None, delta > 0

    # obra não concluída: compara hoje com prevista
    if status not in ("concluida", "cancelada"):
        delta = (hoje - data_fim_prevista).days
        return max(delta, 0) or None, delta > 0

    return None, False


# ---------------------------------------------------------------------------
# Pipeline principal Analytics
# ---------------------------------------------------------------------------

def run_analytics() -> dict:
    """
    Preenche analytics.metricas_obra e analytics.recorrencia_territorial
    a partir de clean.obras.
    """
    counters = {"metricas": 0, "recorrencia": 0}

    with get_session() as session:
        obras = fetch_obras_para_analytics(session)

    logger.info("Analytics: processando %d obras…", len(obras))

    with get_session() as session:
        for obra in obras:
            id_obra = obra["id_obra_geoobras"]

            pct_desembolso = _calc_pct_desembolso(
                obra.get("valor_total_contratado"),
                obra.get("valor_pago_acumulado"),
            )

            dias_atraso, flag_atraso = _calc_dias_atraso(
                obra.get("data_fim_prevista"),
                obra.get("data_fim_real"),
                obra.get("status_obra"),
            )

            metrica: dict[str, Any] = {
                "id_obra_geoobras": id_obra,
                "valor_total_contratado": obra.get("valor_total_contratado"),
                "valor_pago_acumulado": obra.get("valor_pago_acumulado"),
                "percentual_desembolso": pct_desembolso,
                "percentual_fisico": obra.get("percentual_fisico"),
                "data_inicio": obra.get("data_inicio"),
                "data_fim_prevista": obra.get("data_fim_prevista"),
                "data_fim_real": obra.get("data_fim_real"),
                "dias_atraso": dias_atraso,
                "flag_possivel_atraso": flag_atraso,
            }

            upsert_metrica(session, metrica)
            counters["metricas"] += 1

            # Estrutura base de recorrência territorial (sem contagens – Mês 2)
            upsert_recorrencia_territorial(
                session,
                id_obra=id_obra,
                bairro=None,   # bairro será enriquecido no Mês 2
                geom=None,     # geom vem de clean.obras se necessário
            )
            counters["recorrencia"] += 1

    logger.info("Analytics concluído: %s", counters)
    return counters
