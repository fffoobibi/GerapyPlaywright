# -*- coding: utf-8 -*-
import re
import scrapy
from example.items import BookItem
from gerapy_playwright import PlaywrightRequest
import logging
# from playwright.page import Page

logger = logging.getLogger(__name__)


js = '''async () => {
    await new Promise(resolve => setTimeout(resolve, 10000));
    return {
        'name': 'Germey'
    }
}'''


async def execute_action(page):
    await page.evaluate('() => { document.title = "Hello World"; }')
    return 1

from gerapy_playwright.scrapyd_utils import ScrapydUtils

class BookSpider(scrapy.Spider):
    name = 'book'
    allowed_domains = ['spa5.scrape.center']
    base_url = 'https://spa5.scrape.center'

    loop_settings = {
        'enabled': True,
        'frequen': 60
    }

    def loop_task(self):
        self.utils: ScrapydUtils
        self.utils.restart_when(run_gr=120)

    def start_requests(self):
        """
        first page
        :return:
        """
        self.utils = ScrapydUtils().init_spider(self)
        start_url = f'{self.base_url}/page/1'
        logger.info('crawling %s', start_url)
        yield PlaywrightRequest(start_url, callback=self.parse_index, actions=execute_action, wait_for='.item .name', script=js)

    def parse_index(self, response):
        """
        extract books and get next page
        :param response:
        :return:
        """
        logger.debug('response meta %s', response.meta)
        items = response.css('.item')
        for item in items:
            href = item.css('.top a::attr(href)').extract_first()
            detail_url = response.urljoin(href)
            yield PlaywrightRequest(detail_url, callback=self.parse_detail, wait_for='.item .name')

        # next page
        match = re.search(r'page/(\d+)', response.url)
        if not match:
            return
        page = int(match.group(1)) + 1
        next_url = f'{self.base_url}/page/{page}'
        yield PlaywrightRequest(next_url, callback=self.parse_index, wait_for='.item .name')

    def parse_detail(self, response):
        """
        process detail info of book
        :param response:
        :return:
        """
        name = response.css('.name::text').extract_first()
        tags = response.css('.tags button span::text').extract()
        score = response.css('.score::text').extract_first()
        tags = [tag.strip() for tag in tags] if tags else []
        score = score.strip() if score else None
        yield BookItem(name=name, tags=tags, score=score)
