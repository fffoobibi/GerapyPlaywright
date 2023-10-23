from playhouse.pool import PooledMySQLDatabase
from peewee import MySQLDatabase
from playhouse.shortcuts import ReconnectMixin

from scrapy import Spider

__all__ = ("bind_models", "get_db_by_key", "get_db")


class ReconnectMySQLDatabase(ReconnectMixin, MySQLDatabase):
    pass


def bind_models(
        spider,
        key_name: str,
        *models,
        max_connections: int = None,
        stale_timeout: int = 180,
        log_config: bool = False
):
    """绑定model"""
    database = get_db_by_key(
        spider, key_name, max_connections=max_connections, stale_timeout=stale_timeout, log_config=log_config
    )
    database.bind(list(models), bind_refs=False, bind_backrefs=False)
    return database


def get_db_by_spider(spider, settings_name: str, log_config: bool = False) -> ReconnectMySQLDatabase:
    return get_db_by_key(
        spider, settings_name, log_config=log_config
    )


def get_db_by_key(
        spider: Spider, key_name: str, max_connections: int = None, stale_timeout: int = 180, log_config: bool = False
) -> ReconnectMySQLDatabase:
    craler_settings = spider.crawler.settings
    settings = craler_settings.getdict(key_name)
    if max_connections is not None:
        connections = max_connections
    else:
        connections = craler_settings.get("CONCURRENT_REQUESTS")
    db_name = settings.get("db_name")
    if log_config:
        spider.logger.info("init %s database: %s", db_name, settings)
    return get_db(**settings, max_connections=connections, stale_timeout=stale_timeout)


def get_db(
        db_name: str = None,
        user: str = None,
        password: str = None,
        host: str = "localhost",
        port: int = 3306,
        max_connections: int = None,
        stale_timeout: int = 180,
) -> ReconnectMySQLDatabase:
    if db_name is None:
        return ReconnectMySQLDatabase(None)
    return ReconnectMySQLDatabase(
        db_name,
        user=user,
        password=password,
        host=host,
        port=port,
        # max_connections=max_connections,
        # stale_timeout=stale_timeout,
    )
