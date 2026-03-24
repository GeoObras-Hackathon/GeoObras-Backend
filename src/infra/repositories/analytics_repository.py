"""
infra/repositories/analytics_repository.py
Leitura de dados CLEAN e escrita nas tabelas do esquema ANALYTICS.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Leitura CLEAN (para analytics)
# ---------------------------------------------------------------------------

def fetch_obras_para_analytics(session: Session) -> list[dict]:
    sql = text("""
        SELECT
            o.id_obra_geoobras,
            o.valor_total_contratado,
            o.valor_pago_acumulado,
            o.percentual_fisico,
            o.data_inicio,
            o.data_fim_prevista,
            o.data_fim_real,
            o.status_obra
        FROM clean.obras o
    """)
    rows = session.execute(sql).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Escrita ANALYTICS – métricas
# ---------------------------------------------------------------------------

UPSERT_METRICA = text("""
    INSERT INTO analytics.metricas_obra (
        id_obra_geoobras,
        valor_total_contratado, valor_pago_acumulado,
        percentual_desembolso, percentual_fisico,
        data_inicio, data_fim_prevista, data_fim_real,
        dias_atraso, flag_possivel_atraso, calculado_em
    ) VALUES (
        :id_obra,
        :valor_total, :valor_pago,
        :pct_desembolso, :pct_fisico,
        :data_inicio, :data_fim_prevista, :data_fim_real,
        :dias_atraso, :flag_atraso, NOW()
    )
    ON CONFLICT (id_obra_geoobras) DO UPDATE SET
        valor_total_contratado  = EXCLUDED.valor_total_contratado,
        valor_pago_acumulado    = EXCLUDED.valor_pago_acumulado,
        percentual_desembolso   = EXCLUDED.percentual_desembolso,
        percentual_fisico       = EXCLUDED.percentual_fisico,
        dias_atraso             = EXCLUDED.dias_atraso,
        flag_possivel_atraso    = EXCLUDED.flag_possivel_atraso,
        calculado_em            = NOW()
""")


def upsert_metrica(session: Session, m: dict[str, Any]) -> None:
    session.execute(UPSERT_METRICA, {
        "id_obra": str(m["id_obra_geoobras"]),
        "valor_total": m.get("valor_total_contratado"),
        "valor_pago": m.get("valor_pago_acumulado"),
        "pct_desembolso": m.get("percentual_desembolso"),
        "pct_fisico": m.get("percentual_fisico"),
        "data_inicio": m.get("data_inicio"),
        "data_fim_prevista": m.get("data_fim_prevista"),
        "data_fim_real": m.get("data_fim_real"),
        "dias_atraso": m.get("dias_atraso"),
        "flag_atraso": m.get("flag_possivel_atraso", False),
    })


# ---------------------------------------------------------------------------
# Escrita ANALYTICS – recorrência territorial (estrutura base)
# ---------------------------------------------------------------------------

UPSERT_RECORRENCIA = text("""
    INSERT INTO analytics.recorrencia_territorial (id_obra_geoobras, bairro, geom)
    VALUES (:id_obra, :bairro, :geom)
    ON CONFLICT (id_obra_geoobras) DO UPDATE SET
        bairro = EXCLUDED.bairro,
        geom   = EXCLUDED.geom
""")


def upsert_recorrencia_territorial(
    session: Session, id_obra: UUID | str, bairro: str | None, geom: str | None
) -> None:
    session.execute(UPSERT_RECORRENCIA, {
        "id_obra": str(id_obra),
        "bairro": bairro,
        "geom": geom,
    })


# ---------------------------------------------------------------------------
# API – consultas principais
# ---------------------------------------------------------------------------

def query_obras_list(
    session: Session,
    *,
    situacao: str | None = None,
    municipio: str | None = None,
    apenas_com_coordenadas: bool = False,
    apenas_inconsistencias: bool = False,
    valor_minimo: float | None = None,
    page: int = 1,
    page_size: int = 50,
) -> tuple[list[dict], int]:
    """
    Retorna (rows, total_count) para o endpoint GET /obras.
    Filtros básicos; o frontend pode aplicar mais filtros no client-side.
    """
    where_clauses: list[str] = []
    params: dict[str, Any] = {}

    if situacao:
        where_clauses.append("o.status_obra = :situacao")
        params["situacao"] = situacao

    if municipio:
        where_clauses.append("o.municipio ILIKE :municipio")
        params["municipio"] = f"%{municipio}%"

    if apenas_com_coordenadas:
        where_clauses.append("o.latitude IS NOT NULL AND o.longitude IS NOT NULL")

    if apenas_inconsistencias:
        where_clauses.append(
            "(o.flag_data_fim_pendente OR o.flag_populacao_suspeita OR o.flag_empregos_suspeitos)"
        )

    if valor_minimo is not None:
        where_clauses.append("o.valor_total_contratado >= :valor_minimo")
        params["valor_minimo"] = valor_minimo

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    count_sql = text(f"SELECT COUNT(*) FROM clean.obras o {where_sql}")
    total = session.execute(count_sql, params).scalar() or 0

    offset = (page - 1) * page_size
    list_sql = text(f"""
        SELECT
            o.id_obra_geoobras   AS id,
            o.nome,
            o.status_obra        AS status,
            o.data_inicio,
            o.data_fim_prevista,
            o.data_fim_real,
            o.valor_total_contratado,
            o.valor_pago_acumulado,
            o.percentual_fisico,
            m.percentual_desembolso,
            o.latitude,
            o.longitude,
            o.flag_data_fim_pendente,
            o.flag_populacao_suspeita,
            o.flag_empregos_suspeitos,
            m.flag_possivel_atraso,
            o.fonte_principal
        FROM clean.obras o
        LEFT JOIN analytics.metricas_obra m ON m.id_obra_geoobras = o.id_obra_geoobras
        {where_sql}
        ORDER BY o.data_ultima_atualizacao DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """)
    params.update({"limit": page_size, "offset": offset})
    rows = session.execute(list_sql, params).mappings().all()
    return [dict(r) for r in rows], total


def query_obra_detalhe(session: Session, id_obra: str) -> dict | None:
    sql = text("""
        SELECT o.*,
               m.percentual_desembolso, m.dias_atraso, m.flag_possivel_atraso,
               m.calculado_em AS metricas_calculado_em
        FROM clean.obras o
        LEFT JOIN analytics.metricas_obra m ON m.id_obra_geoobras = o.id_obra_geoobras
        WHERE o.id_obra_geoobras = :id
    """)
    row = session.execute(sql, {"id": id_obra}).mappings().first()
    if not row:
        return None

    obra = dict(row)

    # contratos
    contratos_sql = text("""
        SELECT c.*
        FROM clean.contratos c
        JOIN clean.obras_contratos oc ON oc.id_contrato_geoobras = c.id_contrato_geoobras
        WHERE oc.id_obra_geoobras = :id
    """)
    contratos = session.execute(contratos_sql, {"id": id_obra}).mappings().all()
    obra["contratos"] = [dict(c) for c in contratos]
    obra["convenios"] = []  # placeholder Mês 1

    return obra


def query_estatisticas(session: Session) -> dict:
    por_status = session.execute(text("""
        SELECT status_obra, COUNT(*) AS qtd
        FROM clean.obras
        GROUP BY status_obra
        ORDER BY qtd DESC
    """)).mappings().all()

    media_fisico = session.execute(text(
        "SELECT AVG(percentual_fisico) AS media FROM clean.obras WHERE percentual_fisico IS NOT NULL"
    )).scalar()

    atraso_dist = session.execute(text("""
        SELECT flag_possivel_atraso, COUNT(*) AS qtd
        FROM analytics.metricas_obra
        GROUP BY flag_possivel_atraso
    """)).mappings().all()

    return {
        "obras_por_status": [dict(r) for r in por_status],
        "media_percentual_fisico": float(media_fisico) if media_fisico else None,
        "distribuicao_atraso": [dict(r) for r in atraso_dist],
    }


# ---------------------------------------------------------------------------
# ETL log
# ---------------------------------------------------------------------------

def insert_etl_log(
    session: Session,
    fonte: str,
    status: str,
    detalhes: dict | str | None = None,
) -> None:
    import json
    det_json = json.dumps(detalhes, default=str) if isinstance(detalhes, dict) else detalhes
    session.execute(text("""
        INSERT INTO etl_execucao (fonte, status, detalhes)
        VALUES (:fonte, :status, CAST(:detalhes AS jsonb))
    """), {"fonte": fonte, "status": status, "detalhes": det_json})
