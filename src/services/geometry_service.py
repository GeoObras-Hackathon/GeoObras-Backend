"""
services/geometry_service.py
Converte WKT (Well-Known Text) em lat/long e mantém o WKT para o campo geom.
Usa Shapely para parse sem precisar de PostGIS no Python.

NOTA: Se PostGIS estiver ativo no banco, as funções ST_* do Postgres podem
substituir esse serviço. Por enquanto trabalhamos só com WKT + lat/lon numérico.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _try_shapely(wkt: str) -> tuple[float | None, float | None]:
    """Tenta usar Shapely para extrair centroide de qualquer geometry WKT."""
    try:
        from shapely import wkt as shapely_wkt
        geom = shapely_wkt.loads(wkt)
        centroid = geom.centroid
        return centroid.y, centroid.x  # lat = y, lon = x
    except Exception as exc:
        logger.debug("Shapely falhou ao parsear WKT: %s", exc)
        return None, None


def extract_lat_lon(geometria_wkt: Optional[str]) -> tuple[float | None, float | None]:
    """
    Extrai (latitude, longitude) de um WKT.
    Suporta POINT, POLYGON, MULTIPOLYGON etc. via centroide.

    Retorna (None, None) se não conseguir parsear.
    """
    if not geometria_wkt:
        return None, None

    wkt = geometria_wkt.strip()

    # Atalho rápido para POINT simples: "POINT (-41.78 -22.37)"
    if wkt.upper().startswith("POINT"):
        try:
            coords = wkt.split("(")[1].rstrip(")").strip().split()
            lon, lat = float(coords[0]), float(coords[1])
            return lat, lon
        except (IndexError, ValueError):
            pass

    return _try_shapely(wkt)


def wkt_to_geom_text(geometria_wkt: Optional[str]) -> Optional[str]:
    """
    Retorna o WKT normalizado (strip) para armazenar no campo geom (TEXT).
    Se PostGIS estiver disponível no futuro, este campo pode ser convertido
    para geometry com ST_GeomFromText(geom, 4326).
    """
    if not geometria_wkt:
        return None
    return geometria_wkt.strip()
