from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.jobs.careerjet import CareerjetProvider
from app.services.jobs.careerjet import settings as careerjet_settings


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeAsyncClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[dict] = []

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def get(self, url: str, params: dict | None = None) -> _FakeResponse:
        self.calls.append({"url": url, "params": params or {}})
        return _FakeResponse(
            {
                "jobs": [
                    {
                        "title": "Senior Data Scientist",
                        "company": "Example Labs",
                        "locations": "India",
                        "url": "https://example.com/jobs/1",
                        "site": "careerjet",
                        "description": "Build machine learning models with Python, PyTorch, SQL, and MLOps.",
                        "date": "Mon, 21 Apr 2026 10:30:00 GMT",
                    }
                ]
            }
        )


class CareerjetProviderTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self._original_api_key = careerjet_settings.careerjet_api_key
        self._original_candidate_fetch = careerjet_settings.production_live_candidate_fetch
        careerjet_settings.careerjet_api_key = "test-key"
        careerjet_settings.production_live_candidate_fetch = 20

    def tearDown(self) -> None:
        careerjet_settings.careerjet_api_key = self._original_api_key
        careerjet_settings.production_live_candidate_fetch = self._original_candidate_fetch

    async def test_search_returns_empty_without_request_context(self) -> None:
        provider = CareerjetProvider()
        results = await provider.search(query="data scientist", location="India", limit=8, request_context=None)
        self.assertEqual(results, [])

    async def test_search_parses_jobs_with_request_context(self) -> None:
        provider = CareerjetProvider()
        with patch("app.services.jobs.careerjet.httpx.AsyncClient", _FakeAsyncClient):
            results = await provider.search(
                query="data scientist",
                location="India",
                limit=8,
                request_context={"user_ip": "203.0.113.8", "user_agent": "UnitTest/1.0"},
            )
        self.assertEqual(len(results), 1)
        job = results[0]
        self.assertEqual(job["source"], "careerjet")
        self.assertEqual(job["company"], "Example Labs")
        self.assertIn("pytorch", {skill.lower() for skill in job["normalized_data"].get("skills", [])})


if __name__ == "__main__":
    unittest.main()
