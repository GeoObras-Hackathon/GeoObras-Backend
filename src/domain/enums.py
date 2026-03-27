"""
domain/enums.py
Enumerações de domínio: status de obra, fonte de dados, etc.
"""

from enum import Enum


class StatusObra(str, Enum):
    PLANEJADA    = "planejada"
    EM_EXECUCAO  = "em_execucao"
    CONCLUIDA    = "concluida"
    PARALISADA   = "paralisada"
    CANCELADA    = "cancelada"
    INACABADA    = "inacabada"
    DESCONHECIDA = "desconhecida"


class FontePrincipal(str, Enum):
    OBRASGOV = "obrasgov"
    TCE      = "tce"
    MISTA    = "mista"
    CONVENIO = "convenio"


class StatusETL(str, Enum):
    SUCESSO      = "sucesso"
    ERRO_PARCIAL = "erro_parcial"
    ERRO         = "erro"


class FonteETL(str, Enum):
    OBRASGOV  = "obrasgov"
    TCERJ     = "tcerj"
    CONVENIOS = "convenios"
    COMPLETA  = "completa"


# Mapeamento dos valores de situação do ObrasGov → StatusObra
# Chaves em lowercase — normalizar input com .strip().lower() antes do lookup
OBRASGOV_STATUS_MAP: dict[str, StatusObra] = {
    "em execução":            StatusObra.EM_EXECUCAO,
    "em execucao":            StatusObra.EM_EXECUCAO,
    "concluída":              StatusObra.CONCLUIDA,
    "concluida":              StatusObra.CONCLUIDA,
    "paralisada":             StatusObra.PARALISADA,
    "cancelada":              StatusObra.CANCELADA,
    "planejada":              StatusObra.PLANEJADA,
    "em planejamento":        StatusObra.PLANEJADA,
    "cadastrada":             StatusObra.PLANEJADA,
    "em licitação":           StatusObra.PLANEJADA,
    "em licitacao":           StatusObra.PLANEJADA,
    "contratada":             StatusObra.EM_EXECUCAO,
    "inacabada":              StatusObra.INACABADA,
}

TCERJ_STATUS_MAP: dict[str, StatusObra] = {
    "em execução":   StatusObra.EM_EXECUCAO,
    "em execucao":   StatusObra.EM_EXECUCAO,
    "em andamento":  StatusObra.EM_EXECUCAO,
    "concluída":     StatusObra.CONCLUIDA,
    "concluida":     StatusObra.CONCLUIDA,
    "finalizada e com aceitação definitiva emitida": StatusObra.CONCLUIDA,
    "finalizada e com aceitacao definitiva emitida": StatusObra.CONCLUIDA,
    "paralisada":    StatusObra.PARALISADA,
    "cancelada":     StatusObra.CANCELADA,
}
