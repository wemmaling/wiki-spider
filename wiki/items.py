# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WikiItem(scrapy.Item):
    title = scrapy.Field()
    url = scrapy.Field(
        key=True
    )
    image = scrapy.Field()
    detail = scrapy.Field()
    internal_link = scrapy.Field()
    updateAt = scrapy.Field()
