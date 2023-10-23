import requests
import atexit

__all__ = ('init_prate', 'update_prate')


# def _list_projects(logger):
#     # http://localhost:6800/listprojects.json
#     resp = requests.get("http://127.0.0.1:6900/listprojects.json")
#     logger.info("projects: %s", resp.json())


def _list_jobs(spider):
    url = spider.scrapyd_url
    list_job = spider.scrapyd_url_info['listjobs']
    resp = requests.get(url.strip('/') + list_job)
    data = resp.json()
    spider.logger.info("list jobs: %s", data)
    return data


def _restart_spider(spider, project: str):
    # curl http://localhost:6800/schedule.json -d project=myproject -d spider=spider_name
    url = spider.scrapyd_url
    schedule = spider.scrapyd_url_info['schedule']
    schedule_url = url.strip('/') + schedule
    spider.logger.info("schedule spider: %s", spider.name)
    resp = requests.post(schedule_url, data={"project": project, "spider": spider.name}).json()
    spider.logger.info("schedule resp: %s", resp)
    return resp


def _stop_spider(job_id: str, spider, project: str):
    # curl http://localhost:6800/cancel.json -d project=myproject -d job=6487ec79947edab326d6db28a2d86511e8247444
    url = spider.scrapyd_url
    stop_job = spider.scrapyd_url_info['stop']
    spider_name = spider.name
    logger = spider.logger
    logger.info("stop spider: %s", spider_name)
    stop_url = url.strip('/') + stop_job
    resp = requests.post(stop_url, data={"project": project, "job": job_id}).json()
    logger.info("stop resp: %s", resp)
    return resp


def init_prate(spider, scrapyd_url: str):
    spider.prate = 0
    spider.pages_prev = 0
    spider.scrapyd_url = scrapyd_url
    spider.scrapyd_url_info = {
        'stop': '/cancel.json',
        'listprojects': '/listprojects.json',
        'listjobs': '/listjobs.json',
        'schedule': '/schedule.json'
    }
    spider.logger.info('init prate, pages_prev, scrapyd_url: %s, %s, %s', spider.prate, spider.pages_prev, scrapyd_url)


def update_prate(spider, scrapyd_project: str, restart: bool = True):
    self = spider
    pages = self.crawler.stats.get_value("response_received_count", 0)
    prate = pages - self.pages_prev
    self.pages_prev = pages
    self.logger.info("%s crawl %s pages/min", spider.name, prate)
    if prate == 0:
        self.logger.info("try to stop crawl")
        if restart:
            try:
                atexit.unregister(_restart_spider)
            except:
                pass
            atexit.register(_restart_spider, spider, scrapyd_project)
        data = _list_jobs(spider)
        for d in data.get("running", []):
            if d["project"] == scrapyd_project and d["spider"] == self.name:
                _stop_spider(d["id"], self, scrapyd_project)
                _stop_spider(d["id"], self, scrapyd_project)
                _stop_spider(d["id"], self, scrapyd_project)
                _stop_spider(d["id"], self, scrapyd_project)
                break
