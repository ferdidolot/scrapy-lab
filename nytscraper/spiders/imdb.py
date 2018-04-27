# -*- coding: utf-8 -*-
import scrapy
import re

class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/title/tt0096463/fullcredits/']

    def parse(self, response):
        # for actor_link in response.css('td.itemprop a::attr(href)').extract():
        actor_list = []
        link_list = []
        character_list = []
        for actor in response.css('table.cast_list td[itemprop="actor"] span[class="itemprop"]::text ').extract():
            actor_list.append(actor)
            print(actor)
        for link in response.css('table.cast_list td[itemprop="actor"] a::attr(href)').extract():
            link_list.append(link)
            print(link)
        for character in response.xpath('//td[@class="character"]//div//text()').extract():
            if character.strip() :
                temp = re.sub( '\s+', ' ', character.strip(' \t \r \n').replace('\n', ' ') ).strip()
                character_list.append(character)
                print(temp)
        print(len(actor_list))
        print(len(link_list))
        print(len(character_list))