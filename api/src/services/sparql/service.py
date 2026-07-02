import re
from typing import Any

from src.services.common import ServiceError
from src.services.sparql.executor import execute_sparql


async def run_sparql_query(query: str) -> dict[Any, Any]:
    if not query or not query.strip():
        raise ServiceError(400, "A non-empty SPARQL query is required.")

    result = await execute_sparql(query)
    if isinstance(result, dict):
        return result

    if result.startswith("Write operations"):
        raise ServiceError(403, result)

    if result.startswith("GraphDB returned"):
        match = re.search(r"GraphDB returned (\d+)", result)
        status_code = int(match.group(1)) if match else 502
        raise ServiceError(status_code, result)

    raise ServiceError(
        502,
        "Could not reach GraphDB. Check that GRAPHDB_ENDPOINT is configured.",
    )
