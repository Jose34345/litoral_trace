import os
import unittest

from litoral_trace.config import Environment, get_settings


class TestP1TypedSettings(unittest.TestCase):
    def test_named_environments_are_preserved(self):
        old_environment = os.environ.get("ENVIRONMENT")
        os.environ["ENVIRONMENT"] = "staging"

        try:
            settings = get_settings()
            self.assertEqual(settings.environment, Environment.STAGING)
            self.assertTrue(settings.is_staging)
            self.assertFalse(settings.is_test)
            self.assertFalse(settings.is_production)
        finally:
            if old_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = old_environment

    def test_jwt_and_cache_settings_are_typed(self):
        old_environment = os.environ.get("ENVIRONMENT")
        old_secret = os.environ.get("JWT_SECRET_KEY")
        old_algorithm = os.environ.get("JWT_ALGORITHM")
        old_access_expire = os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES")
        old_refresh_expire = os.environ.get("REFRESH_TOKEN_EXPIRE_DAYS")
        old_redis_url = os.environ.get("REDIS_URL")

        os.environ["ENVIRONMENT"] = "development"
        os.environ["JWT_SECRET_KEY"] = "typed-settings-secret-key-123456"
        os.environ["JWT_ALGORITHM"] = "HS256"
        os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "30"
        os.environ["REFRESH_TOKEN_EXPIRE_DAYS"] = "14"
        os.environ["REDIS_URL"] = "redis://cache.internal:6380/2"

        try:
            settings = get_settings()
            self.assertEqual(settings.jwt.algorithm, "HS256")
            self.assertEqual(settings.jwt.access_token_expire_minutes, 30)
            self.assertEqual(settings.jwt.access_token_expire_seconds, 1800)
            self.assertEqual(settings.jwt.refresh_token_expire_days, 14)
            self.assertEqual(
                settings.cache.redis_url,
                "redis://cache.internal:6380/2",
            )
        finally:
            if old_environment is None:
                os.environ.pop("ENVIRONMENT", None)
            else:
                os.environ["ENVIRONMENT"] = old_environment

            if old_secret is None:
                os.environ.pop("JWT_SECRET_KEY", None)
            else:
                os.environ["JWT_SECRET_KEY"] = old_secret

            if old_algorithm is None:
                os.environ.pop("JWT_ALGORITHM", None)
            else:
                os.environ["JWT_ALGORITHM"] = old_algorithm

            if old_access_expire is None:
                os.environ.pop("ACCESS_TOKEN_EXPIRE_MINUTES", None)
            else:
                os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = old_access_expire

            if old_refresh_expire is None:
                os.environ.pop("REFRESH_TOKEN_EXPIRE_DAYS", None)
            else:
                os.environ["REFRESH_TOKEN_EXPIRE_DAYS"] = old_refresh_expire

            if old_redis_url is None:
                os.environ.pop("REDIS_URL", None)
            else:
                os.environ["REDIS_URL"] = old_redis_url


if __name__ == "__main__":
    unittest.main()
