import datetime as dt

from dateutil.relativedelta import relativedelta
from scrapy import FormRequest

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.lib.parser import try_apply
from kp_scrapers.models.customs import CustomsFigure
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.units import Currency, Unit
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.customs.base import CustomsBaseSpider


class KoreaCustomsSpider(LngMarker, LpgMarker, OilMarker, CustomsBaseSpider):
    name = 'KoreaCustoms'
    version = '2.0.0'
    provider = 'KoreaCustoms'
    produces = [DataTypes.CustomsFigure]

    def commodity_mapping(self):
        return {
            'lng': {'271111': None},
            'lpg': {'271112': 'Propane', '271113': 'Butane'},
            'oil': {'270900': None},
        }

    @staticmethod
    def _split_every_n_characters(line, n):
        return tuple(line[i : i + n] for i in range(0, len(line), n))

    def start_requests(self):
        self.logger.info('Rewinding %s months before current month', self.months_look_back)
        past_dates = rewind_time(current_date=dt.date.today(), months=self.months_look_back)
        for commodity, values in self.relevant_commodities().items():
            for past_date in past_dates:
                for code, subcommo in values.items():
                    hsCd, hsCd4, hsCd6 = self._split_every_n_characters(code, 2)
                    for type_ in range(1, 3):
                        url = 'http://www.customs.go.kr/kcshome/trade/TradeCommodityList.do'
                        formdata = {
                            'year': str(past_date.year),
                            'month': "%02d" % (past_date.month,),
                            'hsUnit': '6',
                            'hsCd': hsCd,
                            'hsCd4': hsCd4,
                            'hsCd6': hsCd6,
                            'eximDitc': str(type_),
                        }
                        meta = {
                            'year': past_date.year,
                            'month': past_date.month,
                            'type': type_,
                            'subcommodity': subcommo,
                            'commodity': commodity,
                        }
                        yield FormRequest(url=url, formdata=formdata, meta=meta)

    @validate_item(CustomsFigure, normalize=True, strict=True, log_level='error')
    def parse(self, response):
        for row in response.xpath('//tr')[2:]:
            cells = row.xpath('td/text()').extract()

            # build CustomsFigure model
            start_utc = dt.datetime(response.meta['year'], response.meta['month'], 1)
            item = {
                'start_utc': start_utc,
                'end_utc': start_utc + relativedelta(months=1),
                'product': response.meta['subcommodity'] or response.meta['commodity'],
                'provider_name': self.provider,
                'reported_date': dt.datetime.utcnow()
                .replace(hour=0, minute=0, second=0)
                .isoformat(timespec='seconds'),
                'export_zone': cells[0],
                'import_zone': 'South Korea',  # source is exclusively about korean imports
                'valuation': {
                    'value': try_apply(cells[1].replace(',', ''), int, lambda x: x * 1000),
                    'currency': Currency.USD,
                },
                'mass': try_apply(cells[2].replace(',', ''), int),
                'mass_unit': Unit.kilogram,
            }

            # sanity check; in case no price/mass/export_zone
            if item['export_zone'] != '0' and item['valuation']['value'] != 0 and item['mass'] != 0:
                yield item
