# adapters/__init__.py
"""
adapters/
=========
Top-of-funnel data extraction adapters for external lead sources.

Each adapter:
    1. Hits an external service (RSS, Apify, Firecrawl, etc.)
    2. Downloads and parses the raw payload (JSON, XML, HTML)
    3. Maps every record into a clean JCMPropertyState
    4. Persists to SQLite with pipeline_stage = "sourced"

Once an adapter run completes, the existing main.py pipeline picks up
"sourced" leads at Node 2 without any changes to the locked core nodes.
"""

from __future__ import annotations
import inspect
import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Adapter factories
# ---------------------------------------------------------------------------

def _import_craigslist() -> Callable[..., dict[str, int]]:
    from adapters.craigslist_rss import run_craigslist_adapter
    return run_craigslist_adapter

def _import_apify_facebook() -> Callable[..., dict[str, int]]:
    from adapters.apify_facebook import run_apify_facebook_adapter
    return run_apify_facebook_adapter

def _import_firecrawl_county() -> Callable[..., dict[str, int]]:
    from adapters.firecrawl_county import run_firecrawl_county_adapter
    return run_firecrawl_county_adapter

def _import_zillow_scrape() -> Callable[..., dict[str, int]]:
    from adapters.zillow_scrape import run_zillow_adapter
    return run_zillow_adapter

def _import_bid4assets() -> Callable[..., dict[str, int]]:
    from adapters.bid4assets_auctions import run_bid4assets_adapter
    return run_bid4assets_adapter

_ADAPTER_FACTORIES: dict[str, Callable[[], Callable[..., dict[str, int]]]] = {
    "craigslist_rss":       _import_craigslist,
    "apify_facebook":       _import_apify_facebook,
    "firecrawl_county":     _import_firecrawl_county,
    "zillow_scrape":        _import_zillow_scrape,
    "bid4assets_auctions":  _import_bid4assets,
}

# ---------------------------------------------------------------------------
# Public registry and lookup helpers
# ---------------------------------------------------------------------------

def get_adapter(name: str) -> Callable[..., dict[str, int]]:
    try:
        factory = _ADAPTER_FACTORIES[name]
    except KeyError as exc:
        raise KeyError(f"Unknown adapter: {name!r}") from exc
    return factory()

def list_available_adapters() -> list[str]:
    available: list[str] = []
    for name in _ADAPTER_FACTORIES:
        try:
            get_adapter(name)
            available.append(name)
        except ImportError:
            logger.debug("Adapter %r not yet implemented — skipping.", name)
    return available

def _make_lazy_adapter(name: str) -> Callable[..., dict[str, int]]:
    def _runner(*args, **kwargs) -> dict[str, int]:
        fn = get_adapter(name)
        return fn(*args, **kwargs)
    _runner.__name__ = f"lazy_{name}"
    _runner.__doc__ = f"Lazy wrapper for adapter {name!r}."
    return _runner

REGISTERED_ADAPTERS: dict[str, Callable[..., dict[str, int]]] = {
    name: _make_lazy_adapter(name)
    for name in _ADAPTER_FACTORIES
}

# ---------------------------------------------------------------------------
# Bulk execution helper
# ---------------------------------------------------------------------------

def run_all_adapters(
    dry_run: bool = False,
    max_items: Optional[int] = None,
    only: Optional[list[str]] = None,
    skip: Optional[list[str]] = None,
) -> dict[str, dict[str, int]]:
    
    target_names = list(_ADAPTER_FACTORIES.keys())
    
    if only:
        only_set = set(only)
        target_names = [n for n in target_names if n in only_set]
    if skip:
        skip_set = set(skip)
        target_names = [n for n in target_names if n not in skip_set]

    all_metrics: dict[str, dict[str, int]] = {}

    for name in target_names:
        try:
            fn = get_adapter(name)
        except ImportError:
            logger.info("Skipping unimplemented adapter: %s", name)
            continue
            
        logger.info("Running adapter: %s", name)
        
        try:
            kwargs: dict = {}
            sig = inspect.signature(fn)
            if "dry_run" in sig.parameters:
                kwargs["dry_run"] = dry_run
            if "max_items" in sig.parameters:
                kwargs["max_items"] = max_items
                
            all_metrics[name] = fn(**kwargs)
            logger.info("Adapter %r complete: %s", name, all_metrics[name])
        except Exception as exc:
            logger.exception("Adapter %r raised an unhandled exception: %s", name, exc)
            existing = all_metrics.get(name, {})
            existing["error"] = existing.get("error", 0) + 1
            all_metrics[name] = existing
            
    return all_metrics

__all__ = [
    "REGISTERED_ADAPTERS",
    "get_adapter",
    "list_available_adapters",
    "run_all_adapters",
]
