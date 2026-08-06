import requests


class ExchangeRateService:
    """
    Fetches currency exchange rates from a free public API
    (Frankfurter - no API key required).

    This is used OUTSIDE the Spark DAG - we call it once in plain
    Python before starting any Spark transformation, and pass the
    result (a single float) into Spark as a constant value.

    Why not call the API inside a Spark transformation?
    - Spark would call it once per row (or per worker), which is
      slow, unreliable (network issues), and can hit API rate limits.
    - A currency rate does not change per row anyway - it's the
      same value for the whole batch, so fetching it once is correct.
    """

    BASE_URLS = [
        "https://api.frankfurter.dev/v1/latest",
        "https://api.frankfurter.app/latest",
    ]

    def get_rate(self, from_currency: str, to_currency: str = "USD") -> float:
        """
        Returns the exchange rate to convert 1 unit of from_currency
        into to_currency. Example: get_rate("EUR", "USD") -> 1.08
        """
        params = {"from": from_currency, "to": to_currency}

        last_error = None
        for base_url in self.BASE_URLS:
            try:
                response = requests.get(base_url, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()
                rate = data["rates"][to_currency]
                return float(rate)
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"Failed to fetch exchange rate from all endpoints: {last_error}") from last_error