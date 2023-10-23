import json
import time
import typing as t

from gerapy_playwright.request import PlaywrightRequest
from datetime import timedelta, datetime

try:
    from scrapy_redis.spiders import RedisSpider
    from scrapy_redis.utils import bytes_to_str, is_dict, TextColor

    flag = True
except ImportError:
    flag = False
try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

__all__ = ("RedisPlayWrightSpider",)

if flag:

    class LoopSettings(TypedDict):
        enabled: bool
        frequency: int
        run_now: bool

    class RedisPlayWrightSpider(RedisSpider):
        loop_settings: LoopSettings

        @classmethod
        def from_crawler(cls, crawler, *args, **kwargs):
            obj = super(RedisPlayWrightSpider, cls).from_crawler(
                crawler, *args, **kwargs
            )
            obj.start_time = datetime.now()
            obj.logger.info("spider open %s", obj.start_time)
            obj.spider_opend()
            return obj

        def make_request_from_data(self, data):
            formatted_data = bytes_to_str(data, self.redis_encoding)

            if is_dict(formatted_data):
                parameter = json.loads(formatted_data)
            else:
                self.logger.warning(
                    f"{TextColor.WARNING}WARNING: String request is deprecated, please use JSON data format. \
                    Detail information, please check https://github.com/rmax/scrapy-redis#features{TextColor.ENDC}"
                )
                return PlaywrightRequest(formatted_data, dont_filter=True)

            self.logger.debug("parameter: %s", parameter)

            if parameter.get("url", None) is None:
                self.logger.warning(
                    f"{TextColor.WARNING}The data from Redis has no url key in push data{TextColor.ENDC}"
                )
                return []
            url = parameter.pop("url")
            method = parameter.pop("method").upper() if "method" in parameter else "GET"
            metadata = parameter.pop("meta") if "meta" in parameter else {}
            return self.make_request(url, metadata, method, parameter)

        def make_request(self, url: str, metadata: dict, method: str, data: dict):
            return PlaywrightRequest(
                url, dont_filter=True, method=method, meta=metadata
            )

        def loop_task(self):
            """开启定时任务extension后启用的任务函数"""
            pass

        def call_at(self, at: datetime, func: t.Union[t.Callable, str], *arg, **kwargs):
            """定时开启"""
            from twisted.internet import reactor
            from twisted.internet import task

            if isinstance(func, str):
                func_name = func
                f = getattr(self, func)
            else:
                func_name = func.__name__
                f = func
            self.logger.info("%s call at: %s", func_name, at)
            now = datetime.now()
            delay = (at - now).total_seconds()
            task.deferLater(reactor, delay, f, *arg, **kwargs)

        def run_gr(
            self,
            days=0,
            seconds=0,
            microseconds=0,
            milliseconds=0,
            minutes=0,
            hours=0,
            weeks=0,
            success_reset: bool = False,
        ) -> bool:
            """运行时间超过"""
            delta: timedelta = datetime.now() - self.start_time

            rs = delta >= timedelta(
                days, seconds, microseconds, milliseconds, minutes, hours, weeks
            )
            if rs and success_reset:
                self.start_time = datetime.now()
            return rs

        def crawling_count(self) -> int:
            """当前正在抓取的数量"""
            return len(self.crawler.engine.slot.inprogress)

        def pending_count(self) -> int:
            """待抓取的数量"""
            return len(self.crawler.engine.slot.scheduler)

        def is_idle(self) -> bool:
            return self.crawling_count() == 0 and self.pending_count() == 0

        def exit_scrape(self, reason: str):
            """关闭爬虫"""
            self.crawler.engine.close_spider(self, reason)

        def publish_msg(self, data: dict, channel: str = None):
            """发送消息"""
            pub_channel = channel or f"{self.__class__.__name__}:{self.name}_channel"
            self.server.publish(pub_channel, json.dumps(data, ensure_ascii=False))

        def spider_opend(self):
            pass

else:
    RedisPlayWrightSpider = None
