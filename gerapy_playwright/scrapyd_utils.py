import time

import requests
import atexit
import psutil
from scrapy import Spider


class ScrapydUtils(object):
    def __init__(self):
        self.start_time: int = time.time()
        self.spider: Spider = None
        self.scrapyd_url: str = None
        self.logger = None
        self.project_name: str = None
        self.job_id: str = None
        self.scrapyd_url_info = {
            'stop': '/cancel.json',
            'listprojects': '/listprojects.json',
            'listjobs': '/listjobs.json',
            'schedule': '/schedule.json'
        }

    def get_sys_info(self):
        memory_info = psutil.virtual_memory()
        total_memory = memory_info.total / (1024 ** 3)
        available_memory = memory_info.available / (1024 ** 3)
        used_memory = memory_info.used / (1024 ** 3)
        free_memory = memory_info.free / (1024 ** 3)
        return {
            "total_memory": total_memory,
            "available_memory": available_memory,
            "used_memory": used_memory,
            "free_memory": free_memory,
            "memory_percent": memory_info.percent,
            "cpu_percent": psutil.cpu_percent()
        }

    def _list_jobs(self):
        self.logger.info("list jobs: %s", self.spider.name)
        url = self._get_url('listjobs')
        resp = requests.get(url)
        return resp.json()

    def _get_url(self, key: str):
        return self.scrapyd_url + self.scrapyd_url_info.get(key)

    def _get_job_id(self):
        if self.job_id is None:
            data = self._list_jobs()
            for d in data.get("running", []):
                if d["project"] == self.project_name and d["spider"] == self.spider.name:
                    self.job_id = d["id"]
                    break
        return self.job_id

    def init_spider(self, spider: Spider, project_name: str = None, scrapyd_address: str = None):
        self.spider = spider
        self.logger = spider.logger
        if project_name is None:
            self.logger.info("load SCRAPYD_PROJECT_NAME from settings")
            self.project_name = spider.crawler.settings.get('SCRAPYD_PROJECT_NAME', '')
        else:
            self.project_name = project_name
        if scrapyd_address is None:
            self.logger.info("load SCRAPYD_URL from settings")
            self.scrapyd_url = spider.crawler.settings.get('SCRAPYD_URL', '').strip('/')
        else:
            self.scrapyd_url = scrapyd_address.strip('/')

    def restart(self):
        self.logger.info('restart spider: %s', self.spider.name)
        atexit.unregister(self.schedule_spider)
        atexit.register(self.schedule_spider)
        for i in range(5):
            self.stop_spider()

    def restart_when(self, memory_used_percent_gr: float = 80, run_gr: int = 24 * 3600):
        sys_info = self.get_sys_info()
        self.logger.info("system info: %s", sys_info)
        current_percent = sys_info.get('memory_percent')
        if current_percent >= memory_used_percent_gr:
            f1 = True
        else:
            f1 = False
        if time.time() - self.start_time >= run_gr:
            f2 = True
        else:
            f2 = False
        if f1 or f2:
            self.restart()

    def schedule_spider(self):
        if not self.scrapyd_url:
            self.logger.warn('not set scrapyd url, please use scrapyd manage your scrapy projects')
            import subprocess, os, sys
            from pathlib import Path
            project_directory = os.getcwd()
            spider_name = self.spider.name
            scrapy_path = Path(sys.executable).parent.joinpath('scrapy').absolute().__str__()
            command = [scrapy_path, 'crawl', spider_name]
            try:
                subprocess.check_call(command, cwd=project_directory)
            except subprocess.CalledProcessError as e:
                self.logger.error(f"Error running Scrapy spider: {e}")
        else:
            schedule_url = self._get_url('schedule')
            self.logger.info("schedule spider: %s", self.spider.name)
            resp = requests.post(schedule_url, data={"project": self.project_name, "spider": self.spider.name}).json()
            self.logger.info("schedule resp: %s", resp)
            return resp

    def stop_spider(self):
        self.logger.info("stop spider: %s", self.spider.name)
        if not self.scrapyd_url:
            self.logger.warn('not set scrapyd url, please use scrapyd manage your scrapy projects')
            self.spider.crawler.engine.close_spider(self.spider, 'stop spider by signal')
        else:
            stop_url = self._get_url('stop')
            resp = requests.post(stop_url, data={"project": self.project_name, "job": self._get_job_id()}).json()
            self.logger.info("stop resp: %s", resp)
            return resp
