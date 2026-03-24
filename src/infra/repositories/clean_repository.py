"""
infra/repositories/clean_repository.py
Operações de leitura (RAW) e escrita (CLEAN) nas tabelas do esquema CLEAN.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _jsonb(obj: Any) -> str | None:
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Leitura RAW (para a camada CLEAN consumir)
# ---------------------------------------------------------------------------

def fetch_all_projetos_obrasgov(session: Session) -> list[dict]:
    rows = session.execute(text("SELECT * FROM raw.obrasgov_projetos")).mappings().all()
    return [dict(r) for r in rows]


def fetch_execucao_fisica_latest(session: Session) -> dict[str, dict]:
    """
    Retorna a execução física mais recente por id_unico.
    Resultado: { id_unico: row_dict }
    """
    sql = text("""
        SELECT DISTINCT ON (id_unico) *
        FROM raw.obrasgov_execucao_fisica
        ORDER BY id_unico, data_situacao DESC
    """)
    rows = session.execute(sql).mappings().all()
    return {r["id_unico"]: dict(r) for r in rows}


def fetch_soma_empenhos(session: Session) -> dict[str, float]:
    """Soma total de empenhos por id_projeto_investimento."""
    sql = text("""
        SELECT id_projeto_investimento, COALESCE(SUM(valor_empenho), 0) AS total
        FROM raw.obrasgov_execucao_financeira
        GROUP BY id_projeto_investimento
    """)
    rows = session.execute(sql).mappings().all()
    return {r["id_projeto_investimento"]: float(r["total"]) for r in rows}


def fetch_contratos_obrasgov(session: Session) -> dict[str, list[dict]]:
    """Contratos agrupados por id_projeto_investimento."""
    rows = session.execute(
        text("SELECT * FROM raw.obrasgov_contratos ORDER BY id_projeto_investimento")
    ).mappings().all()
    result: dict[str, list[dict]] = {}
    for r in rows:
        key = r["id_projeto_investimento"]
        result.setdefault(key, []).append(dict(r))
    return result


def fetch_geometria_by_id_unico(session: Session) -> dict[str, dict]:
    """Primeiro registro de geometria por id_unico."""
    sql = text("""
        SELECT DISTINCT ON (id_unico) *
        FROM raw.obrasgov_geometria
        ORDER BY id_unico, data_criacao DESC NULLS LAST
    """)
    rows = session.execute(sql).mappings().all()
    return {r["id_unico"]: dict(r) for r in rows}


def fetch_all_tcerj_obras(session: Session) -> list[dict]:
    rows = session.execute(text("SELECT * FROM raw.tcerj_obras")).mappings().all()
    return [dict(r) for r in rows]


def fetch_all_tcerj_paralisadas_macae(session: Session, municipio: str = "Macaé") -> list[dict]:
    sql = text("SELECT * FROM raw.tcerj_obras_paralisadas WHERE ente ILIKE :ente")
    rows = session.execute(sql, {"ente": f"%{municipio}%"}).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Escrita CLEAN – obras
# ---------------------------------------------------------------------------

UPSERT_OBRA_CLEAN = text("""
    INSERT INTO clean.obras (
        id_obra_geoobras, id_unico_obrasgov, id_obras_tce,
        nome, descricao, municipio, uf, codigo_municipio, bairro, logradouro,
        status_obra, data_inicio, data_fim_prevista, data_fim_real,
        flag_data_fim_pendente, percentual_fisico,
        populacao_beneficiada, flag_populacao_suspeita,
        empregos_gerados, flag_empregos_suspeitos,
        valor_total_contratado, valor_pago_acumulado, valor_previsto_original,
        latitude, longitude, geom, fonte_principal, data_ultima_atualizacao
    ) VALUES (
        :id_obra, :id_obrasgov, :id_tce,
        :nome, :descricao, :municipio, :uf, :cod_municipio, :bairro, :logradouro,
        :status_obra, :data_inicio, :data_fim_prevista, :data_fim_real,
        :flag_data_fim, :percentual_fisico,
        :populacao, :flag_populacao,
        :empregos, :flag_empregos,
        :valor_contratado, :valor_pago, :valor_previsto,
        :latitude, :longitude, :geom, :fonte, NOW()
    )
    ON CONFLICT (id_obra_geoobras) DO UPDATE SET
        status_obra              = EXCLUDED.status_obra,
        data_fim_real            = EXCLUDED.data_fim_real,
        percentual_fisico        = EXCLUDED.percentual_fisico,
        valor_pago_acumulado     = EXCLUDED.valor_pago_acumulado,
        latitude                 = EXCLUDED.latitude,
        longitude                = EXCLUDED.longitude,
        geom                     = EXCLUDED.geom,
        data_ultima_atualizacao  = NOW()
""")


def upsert_obra_clean(session: Session, obra: dict[str, Any]) -> None:
    session.execute(UPSERT_OBRA_CLEAN, {
        "id_obra": obra["id_obra_geoobras"],
        "id_obrasgov": obra.get("id_unico_obrasgov"),
        "id_tce": obra.get("id_obras_tce"),
        "nome": obra["nome"],
        "descricao": obra.get("descricao"),
        "municipio": obra.get("municipio", "Macaé"),
        "uf": obra.get("uf", "RJ"),
        "cod_municipio": obra.get("codigo_municipio"),
        "bairro": obra.get("bairro"),
        "logradouro": obra.get("logradouro"),
        "status_obra": obra.get("status_obra"),
        "data_inicio": obra.get("data_inicio"),
        "data_fim_prevista": obra.get("data_fim_prevista"),
        "data_fim_real": obra.get("data_fim_real"),
        "flag_data_fim": obra.get("flag_data_fim_pendente", False),
        "percentual_fisico": obra.get("percentual_fisico"),
        "populacao": obra.get("populacao_beneficiada"),
        "flag_populacao": obra.get("flag_populacao_suspeita", False),
        "empregos": obra.get("empregos_gerados"),
        "flag_empregos": obra.get("flag_empregos_suspeitos", False),
        "valor_contratado": obra.get("valor_total_contratado"),
        "valor_pago": obra.get("valor_pago_acumulado"),
        "valor_previsto": obra.get("valor_previsto_original"),
        "latitude": obra.get("latitude"),
        "longitude": obra.get("longitude"),
        "geom": obra.get("geom"),
        "fonte": obra.get("fonte_principal"),
    })


# ---------------------------------------------------------------------------
# Escrita CLEAN – contratos
# ---------------------------------------------------------------------------

INSERT_CONTRATO_CLEAN = text("""
    INSERT INTO clean.contratos (
        id_contrato_obrasgov, numero_contrato, objeto,
        valor_contratado, valor_total_atualizado,
        data_inicio_vigencia, data_fim_vigencia,
        municipio, codigo_municipio
    ) VALUES (
        :id_contrato_obrasgov, :numero_contrato, :objeto,
        :valor_contratado, :valor_total_atualizado,
        :data_inicio_vigencia, :data_fim_vigencia,
        :municipio, :codigo_municipio
    )
    RETURNING id_contrato_geoobras
""")

INSERT_OBRA_CONTRATO = text("""
    INSERT INTO clean.obras_contratos (id_obra_geoobras, id_contrato_geoobras, tipo_relacao)
    VALUES (:id_obra, :id_contrato, :tipo_relacao)
    ON CONFLICT DO NOTHING
""")


def insert_contrato_clean(
    session: Session,
    id_projeto: str,
    contrato: dict[str, Any],
    municipio: str = "Macaé",
) -> int:
    result = session.execute(INSERT_CONTRATO_CLEAN, {
        "id_contrato_obrasgov": contrato.get("numero_contrato"),
        "numero_contrato": contrato.get("numero_contrato"),
        "objeto": contrato.get("objeto"),
        "valor_contratado": contrato.get("valor_global"),
        "valor_total_atualizado": contrato.get("valor_acumulado"),
        "data_inicio_vigencia": contrato.get("vigencia_inicio"),
        "data_fim_vigencia": contrato.get("vigencia_fim"),
        "municipio": municipio,
        "codigo_municipio": 3302403,  # IBGE Macaé
    })
    return result.scalar()


def link_obra_contrato(
    session: Session,
    id_obra: UUID | str,
    id_contrato: int,
    tipo: str = "principal",
) -> None:
    session.execute(INSERT_OBRA_CONTRATO, {
        "id_obra": str(id_obra),
        "id_contrato": id_contrato,
        "tipo_relacao": tipo,
    })


# ---------------------------------------------------------------------------
# Escrita CLEAN – convênios
# ---------------------------------------------------------------------------

INSERT_CONVENIO_CLEAN = text("""
    INSERT INTO clean.convenios (
        id_convenio_raw, numero_instrumento, unidade_gestora, aditivo,
        tipo_instrumento, instituicao,
        valor_concedente, valor_convenente, valor_total, municipio
    ) VALUES (
        :id_raw, :numero_instrumento, :unidade_gestora, :aditivo,
        :tipo_instrumento, :instituicao,
        :valor_concedente, :valor_convenente, :valor_total, :municipio
    )
    ON CONFLICT DO NOTHING
""")


def insert_convenio_clean(session: Session, row: dict[str, Any]) -> None:
    session.execute(INSERT_CONVENIO_CLEAN, {
        "id_raw": row.get("id_convenio"),
        "numero_instrumento": row.get("numero_instrumento"),
        "unidade_gestora": row.get("unidade_gestora"),
        "aditivo": row.get("aditivo"),
        "tipo_instrumento": row.get("tipo_instrumento"),
        "instituicao": row.get("instituicao"),
        "valor_concedente": row.get("valor_concedente"),
        "valor_convenente": row.get("valor_convenente"),
        "valor_total": row.get("valor_total"),
        "municipio": "Macaé",
    })
