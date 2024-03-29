import json
import typing
from typing import List, Type, Union

import logging
import pprint

from peewee import Model
from playhouse.pool import PooledMySQLDatabase
# from playhouse.shortcuts import ReconnectMixin

from importlib import import_module

from pymysql.cursors import DictCursor

from twisted.internet import defer
from twisted.enterprise import adbapi

logging.getLogger('peewee').setLevel(logging.WARNING)


def _load_from_str(path: str):
    dot = path.rindex('.')
    module, name = path[:dot], path[dot + 1:]
    m = import_module(module)
    return getattr(m, name)


class PeeweeMySQLPipeLine:
    default = 'MYSQL_SETTINGS'

    def mysql_params(self, settings) -> dict:
        """
        example: return settings.get(self.default)
        """
        raise NotImplementedError

    def model_path(self) -> List[Union[str, Type[Model]]]:
        raise NotImplementedError

    def model_save(self, item, spider, *models: typing.Type[Model]):
        raise NotImplementedError

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self._settings = settings
        self._models: List[Model] = None
        self.database: PooledMySQLDatabase = None

    def open_spider(self, spider):
        self._get_models()
        settings = self.mysql_params(self._settings)
        settings.setdefault('max_connections', 3)
        settings.setdefault('stale_timeout', 300)
        self.database = PooledMySQLDatabase(**settings)
        spider.logger.info('init peewee database')

    def close_spider(self, spider):
        self.database.close()
        spider.logger.info('close peewee database')

    def _get_models(self) -> List[Model]:
        if self._models is None:
            rs = []
            for v in self.model_path():
                if isinstance(v, str):
                    obj = _load_from_str(v)
                else:
                    obj = v
                if obj:
                    rs.append(obj)
            self._models = rs
        return self._models

    def process_item(self, item, spider):
        with self.database.bind_ctx(self._models, bind_refs=False, bind_backrefs=False):
            self.model_save(item, spider, *self._models)
        return item


class ADBMySQLPipeline(object):
    stats_name = 'mysql_pipeline'
    default = 'MYSQL_SETTINGS'

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings, crawler.stats)

    def mysql_params(self, settings) -> dict:
        """
        example: return settings.get(self.default)
        """
        raise NotImplementedError

    def save_table(self) -> str:
        raise NotImplementedError

    def save_item(self, item, spider) -> defer.Deferred:
        """
        return self.insert(item, self.table, spider.logger)
        """
        raise NotImplementedError

    def postprocess_item(self, item, status):
        pass

    def __init__(self, settings, stats):
        self.stats = stats
        self.settings = settings
        self.db: adbapi.ConnectionPool = None
        self.table: str = None

    def open_spider(self, spider):
        self.table = self.save_table()
        self.db = adbapi.ConnectionPool('pymysql', **self.__mysql_params())

    def close_spider(self, spider):
        self.db.close()

    def __mysql_params(self):
        db_args = self.mysql_params(self.settings)
        db_args['cp_reconnect'] = True
        db_args['cursorclass'] = DictCursor
        return db_args

    @defer.inlineCallbacks
    def process_item(self, item, spider):
        status = False
        try:
            yield self.save_item(item, spider)
        except Exception:
            spider.logger.exception('%s', pprint.pformat(item))
            self.stats.inc_value('{}/errors'.format(self.stats_name))
        else:
            status = True  # executed without errors
        self.postprocess_item(item, status)
        yield item

    def _log_db(self, logger, error, action: str):
        logger.error(f'{action} error %s', error)
        self.stats.inc_value(f'{self.stats_name}_{action}/errors')

    def insert_many(self,
                    values: List[dict],
                    table: str = None,
                    logger: typing.Optional[logging.Logger] = None,
                    ignore: bool = True):
        def _insert_many(cursor, table_: str):
            keys = list(values[0].keys())
            len_keys = len(keys)
            ig = 'IGNORE' if ignore else ''
            sql = "INSERT %s INTO %s(%s) VALUES(%s);" % (
                table_ or self.table, ig, ','.join(keys), ','.join(['%s'] * len_keys))
            vs = []
            for v in values:
                vs.append([v[k] for k in keys])
            cursor.executemany(sql, vs)

        defer_ = self.db.runInteraction(_insert_many, table)
        if logger is not None:
            defer_.addErrback(lambda err: self._log_db(logger, err, 'insert many'))
        return defer_

    def upsert(self,
               item: dict,
               preserves: List[str],
               table: str = None,
               logger: typing.Optional[logging.Logger] = None):
        """
        插入或更新
        preserves: 不需要更新的字段
        """

        def _do_insert_or_update(cursor, table: str, keys: List[str], updates: List[str]):
            sql = "INSERT INTO %s(%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (
                table, ','.join(keys),
                ('%s,' * len(keys)).strip(','),
                ''.join(
                    [f'{update}=VALUES({update}), '
                     for update in updates]).strip(
                    ','))
            cursor.execute(sql, tuple(item.values()))

        keys = item.keys()
        updates = set(keys) - set(preserves)
        defer_ = self.db.runInteraction(_do_insert_or_update, table or self.table,
                                        keys, updates)
        if logger is not None:
            defer_.addErrback(lambda err: self._log_db(logger, err, 'upsert'))
        return defer_

    def update(self,
               by: typing.Dict[str, typing.Any],
               item: dict,
               table: str = None,
               logger: typing.Optional[logging.Logger] = None,
               **fields):
        """
        更新
        update({'id=': 20}, {'a': 10, 'b': 20})
        """

        def _adb_update(cursor, sql_, value_):
            cursor.execute(sql_, value_)

        item.update(**fields)

        vals = ','.join([f"{key}=%s" for key in item])
        value = tuple(item.values())
        conditions = ''
        for k, v in by.items():
            conditions += f'{k}{json.dumps(v)} AND '
        c = conditions.rstrip().rstrip('AND')
        sql = f'UPDATE {table or self.table} SET {vals} WHERE {c}'
        defer_ = self.db.runInteraction(_adb_update, sql, value)
        if logger is not None:
            defer_.addErrback(lambda error: self._log_db(logger, error, 'update'))
        return defer_

    def insert(self,
               item: dict,
               table: str = None,
               logger: typing.Optional[logging.Logger] = None,
               ignore: bool = True,
               **fields):
        """
        插入
        """

        def _adb_insert(cursor, sql_, value_):
            cursor.execute(sql_, value_)

        item.update(**fields)
        keys = ','.join(item.keys())
        vals = ','.join(['%s'] * len(item.keys()))
        value = tuple(item.values())
        ig = ' IGNORE ' if ignore else ' '
        sql = f'INSERT{ig}INTO {table or self.table}({keys}) VALUES({vals});'
        defer_ = self.db.runInteraction(_adb_insert, sql, value)
        if logger is not None:
            defer_.addErrback(lambda err: self._log_db(logger, err, 'insert'))
        return defer_

    def delete(self, where: dict, table: str = None, logger: typing.Optional[logging.Logger] = None):
        """
        删除
        """

        def _delete_(cursor, sql_):
            cursor.execute(sql_)

        conditions = ''
        for k, v in where.items():
            conditions += f'{k}{v[0]}{v[1]} AND '
        conditions.rstrip(' AND ')
        sql = f'DELETE FROM {table} WHERE {conditions}'.strip()
        defer_ = self.db.runInteraction(_delete_, sql)
        if logger is not None:
            defer_.addErrback(lambda err: self._log_db(logger, err, 'delete'))
        return defer_

    def add_upsert(self,
                   item: dict,
                   preserves: List[str] = None,
                   table: str = None,
                   expression: str = '',
                   logger: typing.Optional[logging.Logger] = None):
        """
        插入或更新
        preserves: 不需要更新的字段
        """

        def _do_insert_or_update(cursor, table: str, keys: List[str], updates: List[str]):
            sql = "INSERT INTO %s(%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % (
                table, ','.join(keys),
                ('%s,' * len(keys)).strip(','),
                ''.join(
                    [f'{update}=' + f'VALUES({update})' + expression + ','
                     for update in updates]).strip(
                    ',')
            )
            cursor.execute(sql, tuple(item.values()))

        keys = item.keys()
        updates = set(keys) - set(preserves or [])
        defer_ = self.db.runInteraction(_do_insert_or_update, table or self.table,
                                        keys, updates)
        if logger is not None:
            defer_.addErrback(lambda err: self._log_db(logger, err, 'upsert'))
        return defer_
