import scrapy
import re
import datetime
import time
import pymongo
from scrapy import Request
from bs4 import BeautifulSoup
from wiki.items import WikiItem
import collections


# 去除爬取段落中的\n等标签
def stripTagSimple(htmlStr):
    import re
    # self.htmlStr = htmlStr
    blank_line = re.compile('\n+')
    htmlStr = re.sub(blank_line, '', htmlStr)
    dr = re.compile(r'\[.*?\]', re.S)
    htmlStr = re.sub(dr, '', htmlStr)
    xml = re.compile(r'<[^>]+>', re.S)
    htmlStr = re.sub(xml, '', htmlStr)

    return htmlStr


class WikiSpider(scrapy.Spider):
    name = 'wiki_spider'
    allowed_domains = ['wikipedia.org']

    custom_settings = {
        'ITEM_PIPELINES': {
            "wiki.pipelines.WikiPipeline": 300,  # 通过pipeline存入数据库
        },
        'DUPEFILTER_DEBUG': True,
        'MONGO_URI': '10.214.224.142:20000',
        'MONGO_DATABASE': 'crawler_wiki',
    }

    def get_urls(self):
        con = pymongo.MongoClient("10.214.224.142", 20000)
        db = con['crawler_wiki']
        collection = db['brand_list']
        brand_list = []
        for obj in collection.find({}):
            brand_list.append(obj['url'])
            print(obj['url'])
        print(len(brand_list))
        return brand_list

    def start_requests(self):
        # urls = self.get_urls()
        # for url in urls:
        #     yield Request(url=url, callback=self.parse)
        yield Request(url='https://zh.wikipedia.org/zh-cn/Product_Red',
                      callback=self.parse)

    def parse(self, response):
        item = WikiItem()
        title = response.xpath('//h1[@id="firstHeading"]/text()').extract_first()
        item['title'] = title
        item['url'] = response.url
        tr_list = response.xpath('//table[@class="infobox vcard"]/tr')
        r_part = re.compile(r'\[\d.\]|\[\d\]')

        # 右侧的info_box表格
        info_box = []
        for tr in tr_list:
            th = tr.xpath('./th[@scope="row"]//text()').extract_first()
            if th is not None:
                td = re.sub(r_part, "", "".join(tr.xpath('./td//text()').extract()))
                info_box.append({'key': th, 'value': stripTagSimple(td)})
        # print(info_box)
        # print(title)

        image = []
        thumb_tright = response.xpath('//div[@class="thumb tright"]/div[@class="thumbinner"]')
        for pic in thumb_tright:
            img = 'https:' + pic.xpath('./a/img/@src').extract_first()
            img_desc = re.sub(r_part, "", "".join(pic.xpath('./div[@class="thumbcaption"]//text()').extract()))

            image.append({'url': img, 'img_desc': stripTagSimple(img_desc)})
        # print(image)
        item['image'] = image

        html_content = response.xpath('//div[@id="mw-content-text"]').extract_first()
        soup = BeautifulSoup(html_content, 'html.parser')
        # 销毁目录节点
        soup.find('div', class_="toc").decompose()

        # ps是文中所有的段落
        div = soup.find(name='div', class_='mw-parser-output')
        ps = div.find_all('p', recursive=False)  # only direct children
        index = 0
        for p in ps:
            if p.get_text() == '':
                break
            index += 1
        summary = {}
        s_index = 0
        while s_index < index:
            summary[f'{s_index}'] = ps[s_index].get_text()
            s_index += 1
        print(summary)

        start = re.compile(r'<p>', re.DOTALL)
        search_result = start.search(soup.decode('utf-8'))
        if search_result is None:
            search_result = re.compile(r'<h2>', re.DOTALL).search(soup.decode('utf-8'))
        content_text = collections.OrderedDict()
        if search_result is not None:
            start_node = soup.decode('utf-8')[search_result.start():]
            lists = start_node.split('<h2>')

            i = 1
            while i < len(lists):
                lists[i] = '<h2>' + lists[i]
                final_soup = BeautifulSoup(lists[i], 'html.parser')
                para_title = final_soup.find('span', class_="mw-headline").get_text().strip()
                para_contents = final_soup.find_all(['ul', 'p', 'ol', 'table'])
                texts = []
                for para in para_contents:
                    if para.find_all('table'):
                        print('文中有table')
                        for t in para.find_all('table'):
                            texts.append(t)
                    if para.find_all('li'):
                        for li in para.find_all('li'):
                            print(stripTagSimple(li.get_text().strip()))
                            texts.append(li.get_text().strip())
                    else:
                        for p_para in para.find_all('p', recursive=False):
                            texts.append(p_para.get_text().strip())
                content_text[para_title] = texts
                i += 1
            if content_text == {}:
                summary = stripTagSimple(response.xpath('//div[@class="mw-parser-output"]/p').extract_first())
            catlinks = response.xpath('//div[@class="catlinks"]/div[@id="mw-normal-catlinks"]//li')

            tag = {}
            j = 0
            for link in catlinks:
                href = 'https://zh.wikipedia.org' + link.xpath('./a/@href').extract_first()
                cat = link.xpath('./a/text()').extract_first()
                tag[f'{j}'] = cat
                j += 1

            detail = {
                'title': title,
                'summary': summary,
                'infobox': info_box,
                'content': content_text,
                'category': tag,
            }
            item['detail'] = detail
            now_time = datetime.datetime.fromtimestamp(time.time())
            item['updateAt'] = now_time
            return item


def main():
    from scrapy.crawler import CrawlerProcess
    from scrapy.utils.project import get_project_settings

    process = CrawlerProcess(get_project_settings())
    process.crawl(WikiSpider)
    process.start()
    process.stop()


if __name__ == '__main__':
    main()
