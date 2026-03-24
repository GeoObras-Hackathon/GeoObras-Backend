"""
api/schemas.py
Schemas Pydantic v2 para request/response da API REST.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Obras – listagem
# ---------------------------------------------------------------------------

class ObraListItem(BaseModel):
    id: UUID
    nome: str
    status: Optional[str] = None
    data_inicio: Optional[date] = None
    data_fim_prevista: Optional[date] = None
    data_fim_real: Optional[date] = None
    valor_total_contratado: Optional[float] = None
    valor_pago_acumulado: Optional[float] = None
    percentual_fisico: Optional[float] = None
    percentual_desembolso: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    flag_data_fim_pendente: bool = False
    flag_populacao_suspeita: bool = False
    flag_empregos_suspeitos: bool = False
    flag_possivel_atraso: Optional[bool] = None
    fonte_principal: Optional[str] = None

    model_config = {"from_attributes": True}


class ObrasListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ObraListItem]


# ---------------------------------------------------------------------------
# Obras – detalhe
# ---------------------------------------------------------------------------

class ContratoItem(BaseModel):
    id_contrato_geoobras: int
    numero_contrato: Optional[str] = None
    objeto: Optional[str] = None
    valor_contratado: Optional[float] = None
    valor_total_atualizado: Optional[float] = None
    data_inicio_vigencia: Optional[date] = None
    data_fim_vigencia: Optional[date] = None

    model_config = {"from_attributes": True}


class ObraDetalhe(BaseModel):
    id_obra_geoobras: UUID
    id_unico_obrasgov: Optional[str] = None
    id_obras_tce: Optional[int] = None
    nome: str
    descricao: Optional[str] = None
    municipio: Optional[str] = None
    uf: Optional[str] = None
    bairro: Optional[str] = None
    logradouro: Optional[str] = None
    status_obra: Optional[str] = None
    data_inicio: Optional[date] = None
    data_fim_prevista: Optional[date] = None
    data_fim_real: Optional[date] = None
    flag_data_fim_pendente: bool = False
    percentual_fisico: Optional[float] = None
    percentual_desembolso: Optional[float] = None
    populacao_beneficiada: Optional[int] = None
    flag_populacao_suspeita: bool = False
    empregos_gerados: Optional[int] = None
    flag_empregos_suspeitos: bool = False
    valor_total_contratado: Optional[float] = None
    valor_pago_acumulado: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geom: Optional[str] = None
    dias_atraso: Optional[int] = None
    flag_possivel_atraso: Optional[bool] = None
    metricas_calculado_em: Optional[datetime] = None
    fonte_principal: Optional[str] = None
    data_ultima_atualizacao: Optional[datetime] = None
    contratos: list[ContratoItem] = []
    convenios: list[Any] = []

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Estatísticas
# ---------------------------------------------------------------------------

class ObrasPorStatus(BaseModel):
    status_obra: Optional[str] = None
    qtd: int


class DistribuicaoAtraso(BaseModel):
    flag_possivel_atraso: Optional[bool] = None
    qtd: int


class EstatisticasResponse(BaseModel):
    obras_por_status: list[ObrasPorStatus]
    media_percentual_fisico: Optional[float] = None
    distribuicao_atraso: list[DistribuicaoAtraso]


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

class RefreshResponse(BaseModel):
    message: str
    registrado_em: datetime
