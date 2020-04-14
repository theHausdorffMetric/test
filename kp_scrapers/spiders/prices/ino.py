import datetime as dt

from dateutil.parser import parse as parse_date
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.models.items import StockExchangeIndex
from kp_scrapers.spiders.prices import PriceSpider


INDEX = 'HH'
BASE_TICKER = 'HenryHubN'


class InoSpider(PriceSpider):
    name = 'Ino'
    version = '0.1.1'
    # https://www.ino.com/
    provider = 'Ino'

    start_urls = ['http://quotes.ino.com/exchanges/contracts.html?r=NYMEX_NG']

    month_ahead = 5

    def parse(self, response):
        sel = Selector(response)

        table = sel.xpath('//div[@id="col-main"]/div/table')

        for elt in range(0, self.month_ahead):
            last = table.xpath('./tr[{}]/td[6]/text()'.format(elt + 4)).extract_first()
            month = table.xpath('./tr[{}]/td[2]/a/text()'.format(elt + 4)).extract_first()
            month = str(parse_date(month).date())
            ticker = BASE_TICKER + str(elt + 1)

            yield StockExchangeIndex(
                raw_unit='usd/mmbtu',
                raw_value=float(last),
                source_power=1,
                index='HH',
                ticker=ticker,
                zone='United States',
                commodity='lng',
                provider='INO',
                month=month,
                day=str(dt.date.today()),
            )
