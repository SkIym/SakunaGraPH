from typing import Any


def binding_value(binding: dict[Any, Any], key: str, default=None) -> str:
    return binding.get(key, {}).get("value", default)
