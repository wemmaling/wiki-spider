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
        # 'LOG_LEVEL': 'ERROR',
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
        urls = self.get_urls()
        for url in urls:
            yield Request(url=url, callback=self.parse)
        # yield Request(url='https://zh.wikipedia.org/wiki/%E6%84%9B%E9%A6%AC%E4%BB%95',
        #               callback=self.parse)

    def parse(self, response):
        item = WikiItem()
        title = response.xpath('//h1[@id="firstHeading"]/text()').extract_first()
        item['title'] = title
        item['url'] = response.url
        # tr_list = response.xpath('//table[@class="infobox vcard"]/tr')
        tr_list = response.css('.infobox tr')
        image = tr_list.xpath('//a[@class="image"]/img/@src').extract_first()
        if image is not None:
            item['image'] = "https:" + image

        r_part = re.compile(r'\[\d.\]|\[\d\]')

        # 右侧的info_box表格
        info_box = []
        for tr in tr_list:
            th = tr.xpath('./th[@scope="row"]//text()').extract_first()
            if th is not None:
                td = re.sub(r_part, "", "".join(tr.xpath('./td//text()').extract()))
                info_box.append({'key': th, 'value': stripTagSimple(td)})
        print(info_box)
        # print(title)

        pic = []
        thumb_tright = response.xpath('//div[@class="thumb tright"]/div[@class="thumbinner"]')
        for p in thumb_tright:
            if p.xpath('./a/img/@src').extract_first() is not None:
                img = 'https:' + p.xpath('./a/img/@src').extract_first()
                img_desc = re.sub(r_part, "", "".join(p.xpath('./div[@class="thumbcaption"]//text()').extract()))
                pic.append({'url': img, 'img_desc': stripTagSimple(img_desc)})
        # print(pic)
        item['pic'] = pic

        html_content = response.xpath('//div[@id="mw-content-text"]').extract_first()
        soup = BeautifulSoup(html_content, 'html.parser')
        # 销毁目录节点
        catalog = soup.find('div', class_="toc")
        if catalog is not None:
            soup.find('div', class_="toc").decompose()
        # 销毁参考资料节点
        ref = soup.find('ol', class_="references")
        if ref is not None:
            soup.find('ol', class_="references").decompose()

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
            summary[f'{s_index}'] = stripTagSimple(ps[s_index].get_text())
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
                if para_title == "外部链接" or "参考" in para_title:
                    i += 1
                    continue
                para_contents = final_soup.find_all(['p', 'li', 'table'])
                texts = []
                for para in para_contents:
                    if para.name == 'table':
                        texts.append(para.prettify())
                        continue
                    texts.append(stripTagSimple(para.get_text('', True)))
                content_text[para_title.replace('.', '点')] = texts
                i += 1
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
