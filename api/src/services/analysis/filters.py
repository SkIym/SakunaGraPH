import asyncio
import time

from src.schemas.analysis import AnalysisFilterOptionsResponse
from src.services.ontology import get_disaster_taxonomy, get_psgc_nodes

_CACHE_TTL = 300


class _TTLCache:
    def __init__(self) -> None:
        self._entry: tuple[float, AnalysisFilterOptionsResponse] | None = None

    def get(self) -> AnalysisFilterOptionsResponse | None:
        if self._entry is None:
            return None

        timestamp, value = self._entry
        if time.monotonic() - timestamp > _CACHE_TTL:
            self._entry = None
            return None
        return value

    def set(self, value: AnalysisFilterOptionsResponse) -> None:
        self._entry = (time.monotonic(), value)


_cache = _TTLCache()


async def get_filter_options() -> AnalysisFilterOptionsResponse:
    if (cached := _cache.get()) is not None:
        return cached

    locations, disaster_types = await asyncio.gather(
        get_psgc_nodes(),
        get_disaster_taxonomy(),
    )
    result = AnalysisFilterOptionsResponse(
        locations=locations,
        disasterTypes=disaster_types,
    )
    _cache.set(result)
    return result
