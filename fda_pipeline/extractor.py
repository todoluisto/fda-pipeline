"""Extractor — fetches drug approval data from the OpenFDA API.

Uses application-number prefix partitioning to work around the OpenFDA
25,000-record skip limit. Each partition is paginated independently and
results are merged. Retries with exponential backoff on transient failures.
"""

from __future__ import annotations

import logging
import time

import requests

from fda_pipeline import config

logger = logging.getLogger(__name__)


def _build_search_filter(
    partition: str | None = None,
    since_date: str | None = None,
) -> str | None:
    """Combine partition and date filters into an OpenFDA search string.

    Args:
        partition: e.g. "application_number:ANDA0*"
        since_date: YYYYMMDD string for incremental runs.

    Returns:
        Combined search string, or None if no filters.
    """
    parts: list[str] = []
    if partition:
        parts.append(partition)
    if since_date:
        # Brackets MUST be pre-encoded (%5B / %5D) — the OpenFDA API
        # returns HTTP 500 when it receives literal [ ] characters.
        parts.append(
            f"submissions.submission_status_date:%5B{since_date}+TO+99991231%5D"
        )
    if not parts:
        return None
    return "+AND+".join(parts)


def _build_params(skip: int, search_filter: str | None = None) -> dict:
    """Build query parameters for one API page.

    Args:
        skip: Number of records to skip (pagination offset).
        search_filter: Pre-built OpenFDA search string.

    Returns:
        Dict of query parameters.
    """
    params: dict = {
        "limit": config.API_PAGE_SIZE,
        "skip": skip,
    }

    if config.API_KEY:
        params["api_key"] = config.API_KEY

    if search_filter:
        params["search"] = search_filter

    return params


def _build_url(params: dict) -> str:
    """Build the full request URL.

    The search parameter may contain pre-encoded characters (%5B, %5D)
    that must NOT be double-encoded. We build the URL manually so that
    ``requests`` does not re-encode percent signs.
    """
    # Separate search from other params (search needs raw passthrough)
    search = params.pop("search", None)
    parts = [f"{k}={v}" for k, v in params.items()]
    if search:
        parts.append(f"search={search}")
    return f"{config.API_BASE_URL}?{'&'.join(parts)}"


def _request_with_retry(params: dict) -> dict | None:
    """Make a single GET request with retry logic.

    Returns:
        Parsed JSON response dict, or None if all retries failed.
    """
    for attempt in range(1, config.API_MAX_RETRIES + 1):
        try:
            url = _build_url(dict(params))  # copy so we don't mutate original
            resp = requests.get(
                url,
                timeout=60,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            # 404 means no results matched the query — not an error
            if exc.response is not None and exc.response.status_code == 404:
                logger.info("API returned 404 — no matching records for query")
                return None
            logger.warning(
                "HTTP %s on attempt %d/%d: %s",
                status, attempt, config.API_MAX_RETRIES, exc,
            )
        except requests.exceptions.RequestException as exc:
            logger.warning(
                "Request failed on attempt %d/%d: %s",
                attempt, config.API_MAX_RETRIES, exc,
            )

        if attempt < config.API_MAX_RETRIES:
            wait = config.API_RETRY_BACKOFF_BASE ** attempt
            logger.info("Retrying in %.1f seconds…", wait)
            time.sleep(wait)

    logger.error("All %d retries exhausted", config.API_MAX_RETRIES)
    return None


def _extract_partition(search_filter: str | None) -> list[dict]:
    """Paginate through all records matching a single search filter.

    Args:
        search_filter: The OpenFDA search string for this partition.

    Returns:
        List of raw result dicts for this partition.
    """
    label = search_filter or "ALL"
    results: list[dict] = []
    skip = 0

    params = _build_params(skip, search_filter)
    data = _request_with_retry(params)
    if data is None:
        return results

    total = data.get("meta", {}).get("results", {}).get("total", 0)
    page = data.get("results", [])
    results.extend(page)
    logger.info("Partition [%s]: %d total records", label, total)

    skip += config.API_PAGE_SIZE
    while skip < total:
        if skip > 25000:
            logger.error(
                "Partition [%s] exceeds 25K skip limit (total=%d). "
                "This partition needs to be split further.",
                label, total,
            )
            break

        params = _build_params(skip, search_filter)
        data = _request_with_retry(params)
        if data is None:
            logger.error("Pagination stopped early at skip=%d for [%s]", skip, label)
            break

        page = data.get("results", [])
        if not page:
            break

        results.extend(page)
        logger.info(
            "Partition [%s]: skip=%d — %d records (cumulative: %d / %d)",
            label, skip, len(page), len(results), total,
        )
        skip += config.API_PAGE_SIZE

        # Be a good API citizen — small delay between pages
        time.sleep(0.25)

    return results


def extract(since_date: str | None = None) -> list[dict]:
    """Fetch all matching records from the OpenFDA API.

    Uses application-number prefix partitioning to work around the
    OpenFDA 25,000-record skip limit. Each partition is paginated
    independently and results are merged.

    For incremental runs (since_date is set), the date filter is
    combined with each partition filter via AND.

    Args:
        since_date: Optional YYYYMMDD string. If provided, only records
            with submission_status_date >= since_date are returned.

    Returns:
        A list of raw result dicts from the API.
    """
    all_results: list[dict] = []

    for partition in config.API_PARTITIONS:
        search_filter = _build_search_filter(partition, since_date)
        logger.info("Extracting partition: %s", search_filter)
        partition_results = _extract_partition(search_filter)
        all_results.extend(partition_results)
        logger.info(
            "Partition complete: %d records. Running total: %d",
            len(partition_results), len(all_results),
        )

    logger.info(
        "Extraction complete: %d total records fetched across %d partitions",
        len(all_results), len(config.API_PARTITIONS),
    )
    return all_results
