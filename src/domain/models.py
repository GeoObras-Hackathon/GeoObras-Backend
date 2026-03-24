"""
domain/models.py
Modelos de domínio (Pydantic v2) para representar dados em trânsito
entre camadas (RAW → CLEAN → Analytics). Não são mapeamentos ORM.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# RAW – ObrasGov
# ---------------------------------------------------------------------------

class ObraGovRaw(BaseModel):
    id_unico: str
    nome: Optional[str] = None
    cep: Optional[str] = None
    endereco: Optional[str] = None
    descricao: Optional[str] = None
    funcao_social: Optional[str] = None
    meta_global: Optional[str] = None
    data_inicial_prevista: Optional[date] = None
    data_final_prevista: Optional[date] = None
    data_inicial_efetiva: Optional[date] = None
    data_final_efetiva: Optional[date] = None
    data_cadastro: Optional[date] = None
    especie: Optional[str] = None
    natureza: Optional[str] = None
    situacao: Optional[str] = None
    uf: Optional[str] = None
    qtd_empregos_gerados: Optional[int] = None
    populacao_beneficiada: Optional[int] = None
    observacoes_pertinentes: Optional[str] = None
    is_modelada_por_bim: Optional[bool] = None
    tomadores: Optional[list[Any]] = None
    executores: Optional[list[Any]] = None
    repassadores: Optional[list[Any]] = None
    eixos: Optional[list[Any]] = None
    tipos: Optional[list[Any]] = None
    sub_tipos: Optional[list[Any]] = None
    fontes_de_recurso: Optional[list[Any]] = None
    payload_json: Optional[dict[str, Any]] = None


class ExecucaoFisicaRaw(BaseModel):
    id_unico: str
    percentual: Optional[float] = None
    data_situacao: Optional[date] = None
    situacao: Optional[str] = None
    observacoes: Optional[str] = None
    em_operacao: Optional[bool] = None
    justificativa_em_operacao: Optional[str] = None
    cancelamentos_paralisacoes: Optional[list[Any]] = None
    documentos: Optional[list[Any]] = None
    payload_json: Optional[dict[str, Any]] = None


class ExecucaoFinanceiraRaw(BaseModel):
    id_projeto_investimento: str
    nr_nota_empenho: str
    nome_esfera_orcamentaria: Optional[str] = None
    nome_tipo_empenho: Optional[str] = None
    fonte_recurso: Optional[str] = None
    natureza_despesa: Optional[str] = None
    numero_processo: Optional[str] = None
    descricao_empenho: Optional[str] = None
    valor_empenho: Optional[float] = None
    payload_json: Optional[dict[str, Any]] = None


class ContratoRaw(BaseModel):
    id_projeto_investimento: str
    numero_contrato: str
    vigencia_inicio: Optional[date] = None
    vigencia_fim: Optional[date] = None
    data_assinatura: Optional[date] = None
    data_publicacao: Optional[date] = None
    objeto: Optional[str] = None
    processo: Optional[str] = None
    valor_global: Optional[float] = None
    valor_acumulado: Optional[float] = None
    payload_json: Optional[dict[str, Any]] = None


class GeometriaRaw(BaseModel):
    id_unico: str
    geometria_wkt: Optional[str] = None
    geometria_raw: Optional[str] = None
    data_criacao: Optional[date] = None
    origem: Optional[str] = None
    data_metadado: Optional[date] = None
    info_adicionais: Optional[str] = None
    nome_area_executora: Optional[str] = None
    endereco_area_executora: Optional[str] = None
    cep_area_executora: Optional[str] = None
    pais_area_executora: Optional[str] = None
    payload_json: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# RAW – TCE-RJ
# ---------------------------------------------------------------------------

class TCEObraRaw(BaseModel):
    objeto: Optional[str] = None
    empresa: Optional[str] = None
    data_inicio: Optional[date] = None
    previsao_conclusao: Optional[date] = None
    etapas: Optional[str] = None
    percentual_concluido: Optional[float] = None
    situacao: Optional[str] = None
    contratados: Optional[float] = None
    praticados: Optional[float] = None
    registros_atualizados_ate: Optional[date] = None
    motivo_paralisacao: Optional[str] = None
    obra_paralisada: Optional[bool] = None
    payload_json: Optional[dict[str, Any]] = None


class TCEObraParalisadaRaw(BaseModel):
    ano_paralisacao: Optional[int] = None
    data_paralisacao: Optional[date] = None
    tipo_ente: Optional[str] = None
    ente: Optional[str] = None
    nome: Optional[str] = None
    funcao_governo: Optional[str] = None
    numero_contrato: Optional[str] = None
    cnpj_contratada: Optional[str] = None
    nome_contratada: Optional[str] = None
    valor_total_contrato: Optional[float] = None
    valor_pago_obra: Optional[float] = None
    tempo_paralisacao: Optional[str] = None
    motivo_paralisacao: Optional[str] = None
    data_inicio_obra: Optional[date] = None
    status_contrato: Optional[str] = None
    classificacao_obra: Optional[str] = None
    fonte_principal_recursos: Optional[str] = None
    payload_json: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# CLEAN
# ---------------------------------------------------------------------------

class ObraClean(BaseModel):
    id_obra_geoobras: Optional[UUID] = None
    id_unico_obrasgov: Optional[str] = None
    id_obras_tce: Optional[int] = None
    nome: str
    descricao: Optional[str] = None
    municipio: str = "Macaé"
    uf: str = "RJ"
    codigo_municipio: Optional[int] = None
    bairro: Optional[str] = None
    logradouro: Optional[str] = None
    status_obra: Optional[str] = None
    data_inicio: Optional[date] = None
    data_fim_prevista: Optional[date] = None
    data_fim_real: Optional[date] = None
    flag_data_fim_pendente: bool = False
    percentual_fisico: Optional[float] = None
    populacao_beneficiada: Optional[int] = None
    flag_populacao_suspeita: bool = False
    empregos_gerados: Optional[int] = None
    flag_empregos_suspeitos: bool = False
    valor_total_contratado: Optional[float] = None
    valor_pago_acumulado: Optional[float] = None
    valor_previsto_original: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geom: Optional[str] = None   # WKT
    fonte_principal: Optional[str] = None
    data_ultima_atualizacao: Optional[datetime] = None


class MetricaObra(BaseModel):
    id_obra_geoobras: UUID
    valor_total_contratado: Optional[float] = None
    valor_pago_acumulado: Optional[float] = None
    percentual_desembolso: Optional[float] = None
    percentual_fisico: Optional[float] = None
    data_inicio: Optional[date] = None
    data_fim_prevista: Optional[date] = None
    data_fim_real: Optional[date] = None
    dias_atraso: Optional[int] = None
    flag_possivel_atraso: bool = False
