# -*- coding: utf-8 -*-
import scrapy
import unidecode
import re

cleanString = lambda x: '' if x is None else unidecode.unidecode(re.sub(r'\s+',' ',x))

class NytimesSpider(scrapy.Spider):
    name = 'nytimes'
    allowed_domains = ['www.nytimes.com']
    start_urls = ['http://www.nytimes.com/']

    def parse(self, response):
        for article in response.css("section.top-news article.story"):
            article_url = article.css('.story-heading>a::attr(href)').extract_first()
            yield {
                'appears_ulr': response.url,
                'title': cleanString(article.css('.story-heading>a::text').extract_first()),
                'article_url': article_url,
                'author': cleanString(article.css('p.byline::text').extract_first()),
                'summary': cleanString(article.css('p.summary::text').extract_first())+cleanString(' '.join(article.css('ul *::text').extract())),
            }
            next_page = article_url
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse_article)

    def parse_article(self, response):
        yield {
            'appears_ulr': response.url,
            'title': cleanString(response.css('h1.headline::text').extract_first()),
            'author': cleanString(response.css('span.byline-author::text').extract_first()),
            'contents': cleanString(''.join(response.css('div.story-body p.story-body-text::text').extract())),
        }