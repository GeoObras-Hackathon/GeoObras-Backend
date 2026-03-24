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
OBRASGOV_STATUS_MAP: dict[str, StatusObra] = {
    "Em execução":            StatusObra.EM_EXECUCAO,
    "Concluída":              StatusObra.CONCLUIDA,
    "Concluida":              StatusObra.CONCLUIDA,
    "Paralisada":             StatusObra.PARALISADA,
    "Cancelada":              StatusObra.CANCELADA,
    "Planejada":              StatusObra.PLANEJADA,
    "Em planejamento":        StatusObra.PLANEJADA,
    "Inacabada":              StatusObra.INACABADA,
}

TCERJ_STATUS_MAP: dict[str, StatusObra] = {
    "Em execução":   StatusObra.EM_EXECUCAO,
    "Concluída":     StatusObra.CONCLUIDA,
    "Paralisada":    StatusObra.PARALISADA,
    "Cancelada":     StatusObra.CANCELADA,
}
