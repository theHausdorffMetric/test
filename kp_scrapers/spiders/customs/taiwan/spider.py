import datetime as dt

from dateutil.relativedelta import relativedelta
from scrapy import FormRequest

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.models.customs import CustomsFigure
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.units import Currency, Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.customs.base import CustomsBaseSpider
from kp_scrapers.spiders.customs.taiwan import mapping


class TaiwanCustomsSpider(CustomsBaseSpider):
    name = 'TaiwanCustoms'
    version = '2.0.1'
    provider = 'TaiwanCustoms'
    produces = [DataTypes.CustomsFigure]

    start_url = 'https://portal.sw.nat.gov.tw/APGA/GA03_LIST'

    # see https://en.wikipedia.org/wiki/Republic_of_China_calendar
    TAIWAN_CALENDAR_OFFSET = 1911

    def commodity_mapping(self):
        return {
            # products are HS codes
            'lng': {'271111': None},
            'lpg': {'271112': 'Propane', '271113': 'Butane'},
            'oil': {'270900': None},
        }

    def start_requests(self):
        self.logger.info('Rewinding %s months before current month', self.months_look_back)
        past_dates = rewind_time(current_date=dt.date.today(), months=self.months_look_back)
        years = set([past_date.year for past_date in past_dates])

        for commodity, subcommodities in self.relevant_commodities().items():
            # To handle commos with multiple codes
            for subcommo_code, subcommo in subcommodities.items():
                for year in years:
                    # convert from gregorian year to ROC calendar year
                    taiwan_year = year - self.TAIWAN_CALENDAR_OFFSET
                    months = sorted(
                        [past_date.month for past_date in past_dates if past_date.year == year]
                    )
                    formdata = {
                        'maxYearByYear': '102',  # ROC year 102
                        'searchInfo.TypePort': '1',
                        'searchInfo.TypeTime': '0',
                        'searchInfo.StartYear': str(taiwan_year),
                        'searchInfo.StartMonth': str(months[0]),
                        'searchInfo.EndMonth': str(months[-1]),
                        'searchInfo.goodsType': '6',
                        'searchInfo.goodsCodeGroup': subcommo_code,
                        'searchInfo.CountryName': ','.join(mapping.ALL_COUNTRIES),
                    }

                    yield FormRequest(
                        url=self.start_url,
                        formdata=formdata,
                        meta={
                            'year': year,
                            'subcommodity': subcommo,
                            'code': subcommo_code,
                            'commodity': commodity,
                        },
                    )

    @validate_item(CustomsFigure, normalize=True, strict=True, log_level='error')
    def parse(self, response):
        # collect customs figures on a per-month basis
        for month in range(1, 13):
            trs = response.xpath(f'//table[@id="dataList_{month}"]//tr')
            for tr in trs:
                start_utc = dt.datetime(response.meta['year'], month, 1)
                # initialise item
                item = {
                    'start_utc': start_utc,
                    'end_utc': start_utc + relativedelta(months=1),
                    'product': response.meta['subcommodity'] or response.meta['commodity'],
                    'provider_name': self.provider,
                    'reported_date': dt.datetime.utcnow()
                    .replace(hour=0, minute=0, second=0)
                    .isoformat(timespec='seconds'),
                    'import_zone': 'Taiwan',  # source is exclusively about taiwanese imports
                }

                td = tr.xpath('td//text()').extract()
                if len(td) == 9 and td[0] != '全部國家合計' and td[6] != '0':  # LNG items case
                    item['export_zone'] = mapping.COUNTRY_MAPPING.get(td[0])
                    # sanity check, in case we miss out a country mapping
                    if not item['export_zone']:
                        self.logger.error('Unmapped TaiwanCustoms country: %s', td[0])
                        continue

                    item['valuation'] = {
                        # raw currency unit is in "1000USD"
                        'value': try_apply(td[8].replace(',', ''), int, lambda x: x * 1000),
                        'currency': Currency.USD,
                    }
                    item['mass'] = int(td[6].replace(',', ''))
                    item['mass_unit'] = Unit.tons
                    item['volume'] = int(td[4].replace(',', ''))
                    item['volume_unit'] = Unit.cubic_meter
                    yield item

                elif len(td) == 7 and td[0] != '全部國家合計' and td[4] != '0':  # LPG & Oil cases
                    item['export_zone'] = mapping.COUNTRY_MAPPING.get(td[0])
                    if not item['export_zone']:
                        # sanity check, in case we miss out a country mapping
                        self.logger.error('Unknown TaiwanCustoms country: %s', td[0])
                        continue

                    item['valuation'] = {
                        # raw currency unit is in "1000USD"
                        'value': try_apply(td[6].replace(',', ''), int, lambda x: x * 1000),
                        'currency': Currency.USD,
                    }
                    item['mass'] = int(td[4].replace(',', ''))
                    item['mass_unit'] = Unit.tons
                    yield item
