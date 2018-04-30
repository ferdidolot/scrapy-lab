# -*- coding: utf-8 -*-
import scrapy
import re

class ImdbSpider(scrapy.Spider):
    name = 'imdb'
    allowed_domains = ['www.imdb.com']
    start_urls = ['https://www.imdb.com/title/tt0096463/fullcredits/']

    def parse(self, response):
        request = scrapy.Request('https://www.imdb.com/title/tt0096463/fullcredits/',
                                 callback=self.parse_actor_from_movie)
        yield request


    def parse_actor_from_movie(self, response):
        actor_name_list = []
        actor_id_list = []

        movie_name = response.css('h3[itemprop="name"] a::text').extract_first()
        movie_id = response.css('h3[itemprop="name"] a::attr(href)').extract_first().split("/")[2]
        movie_year = re.sub('\s+', ' ', (response.css('h3[itemprop="name"] span[class="nobr"]::text').extract_first()).strip(' \t \r \n').replace('\n', ' ') ).strip()
        movie_year = movie_year.replace("(", "").replace(")","")

        for actor in response.css('table.cast_list td[itemprop="actor"] span[class="itemprop"]::text ').extract():
            actor_name_list.append(actor)

        for link in response.css('table.cast_list td[itemprop="actor"] a::attr(href)').extract():
            actor_id_list.append(link)

        list_actor = []
        count = 0
        for character in response.xpath('//td[@class="character"]//div//text()').extract():
            if character.strip() :
                temp = re.sub( '\s+', ' ', character.strip(' \t \r \n').replace('\n', ' ') ).strip()
                item = dict()

                item['movie_name'] = movie_name
                item['movie_id'] = movie_id
                item['movie_year'] = movie_year
                item['actor_id'] = actor_id_list[count].split("/")[2]
                item['actor_name'] = actor_name_list[count]
                item['role_name'] = temp
                item['link'] = actor_id_list[count]
                request = scrapy.Request('https://www.imdb.com/name/' + item['actor_id'] + '/bio',
                                         callback=self.parse_actor_bio)
                request.meta['item'] = item
                yield request

                count = count + 1

    def parse_actor_bio(self, response):
        birth_date = response.css('td time::attr(datetime)').extract()
        height = response.css('table[id="overviewTable"] td::text' ).extract()
        if height:
            height = height[-1].strip()
        else:
            height = ""
        if not any(char.isdigit() for char in height):
            height = ""
        item = response.meta['item']
        if birth_date:
            item['birth_date'] = birth_date[0]
        else:
            item['birth_date'] = ""
        item['height'] = height

        yield item



