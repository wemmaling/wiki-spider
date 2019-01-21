import scrapy
import re
from scrapy import Request
import pymongo
from urllib.parse import unquote


class CatSpider(scrapy.Spider):
    name = 'cat_spider'
    allowed_domains = ['wikipedia.org']

    def __init__(self):
        self.brand_list = []

    def start_requests(self):
        yield Request(url="https://zh.wikipedia.org/zh-cn/Category:%E5%94%B1%E7%89%87%E5%85%AC%E5%8F%B8%E6%A8%A1%E6%9D%BF",
                      callback=self.parse)

    def parse(self, response):
        con = pymongo.MongoClient("10.214.224.142", 20000)
        db = con['crawler_wiki']
        collection = db['brand_list']
        current_cat = re.search('https://zh.wikipedia.org/zh-cn/Category:(.*?)$', response.url).group(1)
        brand_li = response.xpath(
            '//div[@id="mw-pages"]//div[@class="mw-category"]/div[@class="mw-category-group"]//li')
        for li in brand_li:
            title = li.xpath('./a/text()').extract_first()
            url = 'https://zh.wikipedia.org/zh-cn' + re.search('/wiki(.*?)$', li.xpath('./a/@href').extract_first()).group(1)
            collection.remove({'title': title, 'url': url})
            self.brand_list.append({'title': title, 'url': url, 'category': current_cat})
        sub_cats = response.xpath(
            '//div[@id="mw-subcategories"]//div[@class="mw-category"]//li//a[@class="CategoryTreeLabel'
            '  CategoryTreeLabelNs14 CategoryTreeLabelCategory"]')
        for cat in sub_cats:
            cat_url = 'https://zh.wikipedia.org/zh-cn' + re.search('/wiki(.*?)$', cat.xpath('./@href').extract_first()).group(1)
            print(cat_url)
            yield Request(url=cat_url, callback=self.parse)
        # print(self.brand_list)
        # for e in self.brand_list:
        #     collection.insert({'title': e.get('title'), 'url': e.get('url'), 'category': unquote(e.get('category'))})


def main():
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(CatSpider)
    process.start()
    process.stop()


if __name__ == '__main__':
    main()
