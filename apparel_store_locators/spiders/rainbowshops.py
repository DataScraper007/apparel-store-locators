import json
import re
from datetime import datetime

import scrapy
from scrapy.cmdline import execute

from apparel_store_locators.items import ApparelStoreLocatorsItem


class RainbowshopsSpider(scrapy.Spider):
    name = "rainbowshops"

    def start_requests(self):
        yield scrapy.Request(
            url="https://stores.rainbowshops.com/",
            # headers=self.headers,
            # cookies=self.cookies,
            callback=self.parse_states
        )

    def parse_states(self, response, **kwargs):
        state_urls = response.xpath('//div[@class="state"]/a/@href').getall()

        for url in state_urls:
            yield scrapy.Request(url='https://stores.rainbowshops.com' + url, callback=self.parse_stores)

    def parse_stores(self, response, **kwargs):
        store_info = response.xpath('//div[@class="state-infobox-title"]')

        for info in store_info:
            end_point = info.xpath('./a/@href').get()
            yield scrapy.Request(
                url = "https://stores.rainbowshops.com" + end_point,
                callback = self.parse
            )

    def parse(self, response, **kwargs):
        raw_data = response.xpath('//script[@type="application/ld+json"]/text()').get()
        json_data = json.loads(raw_data)

        item = ApparelStoreLocatorsItem()
        item['name'] = json_data['name']
        item['url'] = json_data['url']
        item['store_id'] = self.extract_store_id(item['url'])
        item['phone'] = json_data['telephone']
        address_data = json_data['address']
        item['street'] = address_data['streetAddress']
        item['city'] = address_data['addressLocality']
        item['state'] = address_data['addressRegion']
        item['country'] = address_data['addressCountry']
        item['latitude'] = json_data['geo']['latitude']
        item['longitude'] = json_data['geo']['longitude']
        item['open_hours'] = self.extract_opening_hours(json_data)
        item['county'] = ''
        item['zip_code'] = self.extract_zip_code(response)
        item['status'] = self.extract_status(response)
        item['provider'] = 'Rainbow Shops'
        item['category'] = 'Apparel'
        item['updated_date'] = datetime.today().strftime('%d-%m-%Y')
        item['direction_url'] = response.xpath('//div[@class="get-directions-storepage"]//a[contains(@href, "maps")]/@href').get('')
        yield item
    def extract_opening_hours(self, json_data):
        opening_hours = json_data['openingHoursSpecification']
        # Parse and format
        formatted_hours = []
        for schedule in opening_hours:
            days = schedule["dayOfWeek"]
            opens = schedule["opens"]
            closes = schedule["closes"]
            for day in days:
                formatted_hours.append(f"{day}: {opens}-{closes}")

        # Join all into a single string
        return " | ".join(formatted_hours)

    def extract_store_id(self, url):
        match = re.search(r"/(\d+)/?$", url)
        return match.group(1) if match else ''

    def extract_status(self, response):
        status = response.xpath('//span[@class="open-now"]/text()').get()
        return "OPEN" if status else "CLOSE"

    def extract_zip_code(self, response):
        address_data = response.xpath('//div[@class="loc-address"]/div/text()').getall()
        address = ' '.join(address_data)
        zip_code = re.search(r'\b\d{5}\b', address)
        return zip_code.group() if zip_code else ''


if __name__ == '__main__':
    execute(f'scrapy crawl {RainbowshopsSpider.name}'.split())
