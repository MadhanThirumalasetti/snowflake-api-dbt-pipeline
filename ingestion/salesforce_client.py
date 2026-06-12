import requests
import time
import logging
from typing import Generator, Optional

logger = logging.getLogger(__name__)


class SalesforceClient:
    """
    REST API client for Salesforce CRM ingestion.
    Handles OAuth 2.0 token refresh, cursor-based pagination,
    and per-endpoint rate limiting with retry logic.
    """

    TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2  # seconds

    def __init__(self, client_id: str, client_secret: str, username: str, password: str, instance_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.instance_url = instance_url
        self._access_token: Optional[str] = None

    def _authenticate(self) -> str:
        """Fetch OAuth 2.0 access token using password flow."""
        resp = requests.post(self.TOKEN_URL, data={
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
        })
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]
        logger.info("Salesforce OAuth token refreshed successfully")
        return self._access_token

    def _get_headers(self) -> dict:
        if not self._access_token:
            self._authenticate()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def _request_with_retry(self, url: str, params: dict = None) -> dict:
        """
        Execute GET request with retry logic and token refresh on 401.
        Backs off exponentially on rate limit (429) responses.
        """
        for attempt in range(self.MAX_RETRIES):
            resp = requests.get(url, headers=self._get_headers(), params=params)

            if resp.status_code == 401:
                logger.warning("Token expired, refreshing...")
                self._authenticate()
                continue

            if resp.status_code == 429:
                wait = self.RETRY_BACKOFF ** attempt
                logger.warning(f"Rate limited. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        raise Exception(f"Max retries exceeded for URL: {url}")

    def fetch_opportunities(self, batch_size: int = 200) -> Generator[list, None, None]:
        """
        Paginate through Salesforce Opportunities using cursor-based pagination.
        Yields batches of records for downstream loading into Snowflake.
        """
        soql = (
            f"SELECT Id, Name, StageName, Amount, CloseDate, AccountId, "
            f"OwnerId, CreatedDate, LastModifiedDate "
            f"FROM Opportunity ORDER BY LastModifiedDate ASC LIMIT {batch_size}"
        )
        url = f"{self.instance_url}/services/data/v57.0/query"
        params = {"q": soql}

        while url:
            data = self._request_with_retry(url, params=params)
            records = data.get("records", [])
            logger.info(f"Fetched {len(records)} opportunity records")
            yield records

            # Salesforce returns nextRecordsUrl for cursor pagination
            next_url = data.get("nextRecordsUrl")
            url = f"{self.instance_url}{next_url}" if next_url else None
            params = None  # params only needed on first request
