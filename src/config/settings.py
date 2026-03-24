"""
config/settings.py
Centraliza todas as configurações do projeto (URLs, parâmetros, DSN).
Use um arquivo .env na raiz do projeto para sobrescrever variáveis.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # ------------------------------------------------------------------
    # Banco de dados
    # ------------------------------------------------------------------
    DATABASE_URL: str = (
        "postgresql://geoobras:geoobras@localhost:5432/geoobras"
    )

    # ------------------------------------------------------------------
    # ObrasGov.br
    # ------------------------------------------------------------------
    OBRASGOV_BASE_URL: str = "https://api.obrasgov.gestao.gov.br/obrasgov/api"
    OBRASGOV_UF: str = "RJ"
    OBRASGOV_PAGE_SIZE: int = 100
    # SUPOSIÇÃO: filtramos município "Macaé" na camada CLEAN.
    # A API não tem filtro direto de município, então trazemos todo o RJ.
    OBRASGOV_MUNICIPIO_ALVO: str = "Macaé"

    # ------------------------------------------------------------------
    # TCE-RJ
    # ------------------------------------------------------------------
    TCERJ_BASE_URL: str = "https://dados.tcerj.tc.br/api/v1"
    TCERJ_PAGE_SIZE: int = 1000
    # Anos para buscar obras paralisadas
    TCERJ_ANOS_PARALISADAS: list[int] = [2020, 2021, 2022, 2023, 2024]

    # ------------------------------------------------------------------
    # Convênios CSV
    # ------------------------------------------------------------------
    CONVENIOS_DIR: str = "data/input/macae_convenios"

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------
    LOG_LEVEL: str = "INFO"
    HTTP_TIMEOUT: float = 30.0    # segundos por requisição
    HTTP_MAX_RETRIES: int = 3

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()
