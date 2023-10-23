from scrapy import signals
from twisted.internet.task import LoopingCall


class LoopingTaskExtension(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        loop_enabled = settings.get("LOOP_ENABLED", False)
        loop_frequency = settings.get("LOOP_FREQUENCY", 0)
        run_now = settings.get("LOOP_RUNNOW", False)
        ext = cls(loop_enabled, loop_frequency, run_now)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def __init__(self, loop_enabled: bool, loop_frequency: int, run_now: bool):
        self._loop_tasks_ = []
        self.loop_enabled = loop_enabled
        self.loop_frequency = loop_frequency
        self.run_now = run_now

    def spider_opened(self, spider):
        loop_settings = getattr(spider, "loop_settings", {})
        
        spider_enabled = loop_settings.get("enabled", False)
        spider_frequency = loop_settings.get("frequency", 0)
        spider_run_now = loop_settings.get("run_now", False)

        if spider_enabled is not None:
            enabled = spider_enabled
        else:
            enabled = self.loop_enabled

        if spider_frequency is not None:
            frequency = spider_frequency
        else:
            frequency = self.loop_frequency

        if spider_run_now is not None:
            run_now = spider_run_now
        else:
            run_now = self.run_now
        task_func = getattr(spider, "loop_task", None)
        if enabled and frequency and task_func:
            loop = LoopingCall(spider.loop_task)
            defer = loop.start(frequency, run_now)
            self._loop_tasks_.append(defer)
            defer.addErrback(
                lambda e: spider.logger.error(
                    f"{spider.name} error in looping task {e}", exc_info=True
                )
            )
            spider.logger.info(f"{spider.name} set looping task success")

            # from twisted.internet import reactor, defer as defered
            # # 创建一个异步任务
            # def execute_async_task():
            #     d = defered.Deferred()

            #     def async_task_callback(result):
            #         # 异步任务完成后的回调函数
            #         d.callback(result)

            #     # 在Twisted的线程池中执行异步任务
            #     reactor.callInThread(run_async_task, async_task_callback)
            #     return d

            # # 异步任务
            # def run_async_task(callback):
            #     # 在此处执行异步操作，例如异步数据库查询或其他非阻塞任务
            #     import asyncio
            #     queue = asyncio.Queue()
            #     async def my_async_task():
            #         await asyncio.sleep(2)  # 模拟一个异步任务
            #         print('block here')
            #         while 1:
            #             await queue.get()
            #         return "Task completed asynchronously"

            #     loop = asyncio.new_event_loop()
            #     result = loop.run_until_complete(my_async_task())
            #     loop.close()

            #     callback(result)

            # # 处理异步任务的结果
            # def handle_result(result):
            #     spider.logger.info(f"Result: {result}")

            # self._loop_tasks_.append(execute_async_task())

    def spider_closed(self, spider):
        """nothing to do"""
