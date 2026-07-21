"""Public NDRRMC parser entry point grouped behind source-owned modules."""

from ._parser import *  # noqa: F401,F403
from ._parser import main


if __name__ == "__main__":
    raise SystemExit(main())
