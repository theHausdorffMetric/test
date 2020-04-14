# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import date
import re

from scrapy.http import FormRequest
from scrapy.selector import Selector
import six
from six.moves import zip

from kp_scrapers.lib.date import rewind_time
from kp_scrapers.models.items import Customs
from kp_scrapers.spiders.bases.markers import LngMarker, LpgMarker, OilMarker
from kp_scrapers.spiders.customs.base import CustomsBaseSpider


class NorwayCustomsSpider(LngMarker, LpgMarker, OilMarker, CustomsBaseSpider):
    name = 'NorwayCustoms'
    version = '1.0.0'
    provider = 'NorwayCustoms'

    def commodity_mapping(self):
        return {
            'lng': {'27111100': None},
            'lpg': {'27111200': 'propane', '27111300': 'butane'},
            'oil': {'27090009': None},
        }

    def start_requests(self):
        url = 'https://www.ssb.no/statistikkbanken/SelectVarVal/saveselections.asp'
        self.logger.info('Rewinding %s months before current month' % self.months_look_back)
        past_dates = rewind_time(current_date=date.today(), months=self.months_look_back)
        years = set([past_date.year for past_date in past_dates])
        if not self.products:
            return
        raw_body = (
            'CMSSubjectArea=&KortNavnWeb=muh&StatVariant=&tidrubr=&MainTable=UhMdVareLand&SubTable=1&SelCont=&SubjectCode=01&SubjectArea=&ProductId=01&antvar=4&action=urval&nvl=True&gruppeform=&valgtecellerEnkel=2&valgtecellerEnkel2=av++3&Contents=Mengde1&Contents=Verdi&TS=ShowTable%26OldTab%3DSelect%26SubjectCode%3D01%26AntVar%3D4%26Contents%3DMengde1%26tidrubr%3D&resetvar=&resetval=&resetnr=&PLanguage=0&FF=2&OldTab=Select&FokusertBoks=&SummerBar=True&mt=0&pm=&TargetSelValues=https%253A%252F%252Fwww%252Essb%252Eno%252Fstatistikkbanken%252FSelectVarVal%252FDefine%252Easp%253FSubjectCode%253D01%2526ProductId%253D01%2526MainTable%253DUhMdVareLand%2526SubTable%253D1%2526PLanguage%253D0%2526Qid%253D0%2526nvl%253DTrue%2526mt%253D0%2526pm%253D%2526gruppe1%253DHele%2526gruppe2%253DHele%2526gruppe3%253DHele%2526aggreg1%253D%2526aggreg2%253D%2526aggreg3%253D%2526VS1%253DVareKoderKN8Siff%2526VS2%253DImpEks%2526VS3%253DLandKoderAlf1%2526CMSSubjectArea%253D%2526KortNavnWeb%253Dmuh%2526StatVariant%253D%2526TabStrip%253DSelect%2526checked%253Dtrue&Qid=0&V1=Varekoder&VS1=VareKoderKN8Siff&VP1=varenummer&VE1=A&V2=ImpEks&VS2=ImpEks&VP2=import%2Feksport&VE2=S&V3=Land&VS3=LandKoderAlf1&VP3=land&VE3=A&V4=Tid&VS4=&VP4=&VE4=&valgtecellerEnkel=1&valgtecellerEnkel2=av++10183&valgtecellerEnkel=1&valgtecellerEnkel2=av++2&valgtecellerEnkel=261&valgtecellerEnkel2=av++261&gruppeVerdimengde1=VM%BFVareKoderKN8Siff&gruppeVerdimengde3=VM%BFLandKoderAlf1&aggreg1=&aggreg3=&var1={}&var2=2'  # noqa
            '&var3=AF&var3=AL&var3=DZ&var3=AS&var3=AD&var3=AO&var3=AI&var3=AG&var3=AN&var3=AR&var3=AM&var3=AW&var3=AZ&var3=AU&var3=BS&var3=BH&var3=BD&var3=BB&var3=BE&var3=BZ&var3=BJ&var3=BM&var3=BT&var3=BO&var3=BQ&var3=BA&var3=BW&var3=BR&var3=BN&var3=BG&var3=BF&var3=BU&var3=BI&var3=CA&var3=KY&var3=XC&var3=CL&var3=CX&var3=CO&var3=CK&var3=CR&var3=CU&var3=CW&var3=DK&var3=VI&var3=VG&var3=AE&var3=DO&var3=CF&var3=IO&var3=DJ&var3=DM&var3=EC&var3=EG&var3=GQ&var3=SV&var3=CI&var3=ER&var3=EE&var3=ET&var3=FK&var3=FJ&var3=PH&var3=FI&var3=FR&var3=GF&var3=PF&var3=TF&var3=FO&var3=GA&var3=GM&var3=GE&var3=GH&var3=GI&var3=GD&var3=GL&var3=GP&var3=GU&var3=GT&var3=GG&var3=GN&var3=GW&var3=GY&var3=HT&var3=HM&var3=GR&var3=HN&var3=HK&var3=BY&var3=IN&var3=ID&var3=IQ&var3=IR&var3=IE&var3=IS&var3=IM&var3=IL&var3=IT&var3=JM&var3=JP&var3=YE&var3=JE&var3=JO&var3=YU&var3=KH&var3=CM&var3=XB&var3=CV&var3=KZ&var3=KE&var3=CN&var3=KG&var3=KI&var3=CC&var3=KM&var3=CD&var3=CG&var3=XK&var3=HR&var3=KW&var3=CY&var3=LA&var3=LV&var3=LS&var3=LB&var3=LR&var3=LY&var3=LI&var3=LT&var3=LU&var3=MO&var3=MG&var3=MK&var3=MW&var3=MY&var3=MV&var3=ML&var3=MT&var3=MA&var3=MH&var3=MQ&var3=MR&var3=MU&var3=YT&var3=MX&var3=FM&var3=MD&var3=MC&var3=MN&var3=ME&var3=MS&var3=MZ&var3=MM&var3=NA&var3=NR&var3=NL&var3=NP&var3=NZ&var3=NI&var3=NE&var3=NG&var3=NU&var3=KP&var3=MP&var3=NF&var3=NC&var3=NT&var3=OM&var3=PK&var3=PW&var3=PS&var3=PA&var3=PG&var3=PY&var3=PE&var3=PN&var3=PL&var3=PT&var3=PR&var3=QA&var3=RE&var3=RO&var3=RU&var3=RW&var3=BL&var3=KN&var3=LC&var3=MF&var3=VC&var3=PM&var3=SB&var3=WS&var3=SM&var3=ST&var3=SA&var3=SN&var3=RS&var3=CS&var3=SC&var3=SL&var3=SG&var3=SX&var3=SK&var3=SI&var3=SO&var3=SU&var3=ES&var3=LK&var3=SH&var3=GB&var3=SD&var3=SR&var3=CH&var3=SE&var3=SZ&var3=SY&var3=GS&var3=ZA&var3=KR&var3=SS&var3=TJ&var3=TW&var3=TZ&var3=TH&var3=TG&var3=TK&var3=TO&var3=TT&var3=TD&var3=CZ&var3=CS_&var3=TN&var3=TM&var3=TC&var3=TV&var3=TR&var3=DE&var3=UG&var3=UA&var3=HU&var3=UY&var3=US&var3=UM&var3=UZ&var3=VU&var3=VA&var3=VE&var3=XI&var3=EH&var3=VN&var3=WF&var3=YD&var3=ZR&var3=ZM&var3=ZW&var3=AT&var3=TP&var3=TL&var3=DD&var3=AX&var3=ZZ'  # noqa
            '&valgtecellerEnkel=12&valgtecellerEnkel=+av+324&rubrik4=m%E5ned'
            '&valgteceller=6264&Forward=Vis+tabell+%3E%3E'.format(
                '&var1='.join([str(p) for p in self.products.keys()])
            )
        )

        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        for year in years:
            months = sorted([past_date.month for past_date in past_dates if past_date.year == year])
            month_params = ''.join(
                ['&var4=' + str(year) + 'M%02d' % (month,) for month in reversed(months)]
            )
            body = raw_body + month_params
            yield FormRequest(
                url=url, body=body, headers=headers, meta={'year': year, 'months': months}
            )

    def parse(self, response):
        sel = Selector(response)
        trs = sel.xpath('//tr[td[@class="stub1" or @class="stub3"]]')
        subcommodities = None
        for tr in trs:
            product_code = tr.xpath('td[@class="stub1"]/text()').extract()
            if product_code:
                product_code = str(re.search(r'\d+', product_code[0]).group())
                product_code = self.get_product(product_code)
                subcommodities = self.get_subcommodities(product_code)
                continue

            raw_data = tr.xpath('td/text()').extract()
            for month, record in enumerate(zip(raw_data[1::2], raw_data[2::2])):
                if record[0] != '0':
                    for subcommo, commodity in six.iteritems(subcommodities):
                        item = Customs()
                        item['commodity'] = subcommo or commodity
                        item['url'] = response.url
                        item['type'] = 'Export'
                        item['raw_weight'] = int(record[0].replace(' ', ''))
                        item['raw_weight_units'] = 'kg'
                        item['raw_price'] = int(record[1].replace(' ', ''))
                        item['raw_price_currency'] = 'NOK'
                        item['year'] = int(response.meta['year'])
                        item['month'] = int(response.meta['months'][month])
                        item['country_name'] = raw_data[0]
                        yield item
