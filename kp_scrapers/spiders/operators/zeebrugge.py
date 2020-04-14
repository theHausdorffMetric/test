# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
from datetime import timedelta

from scrapy.http import FormRequest, Request
from scrapy.selector import Selector

from kp_scrapers.spiders.bases.persist import PersistSpider

from . import OperatorSpider
from .extractors.zeebrugge import ExcelExtractorZeebrugge


ONE_YEAR = timedelta(days=365)


class ZeebruggeSpider(OperatorSpider, PersistSpider):

    name = 'ZeebruggeOperator'

    allowed_domains = ['gasdata.fluxys.com']
    start_urls = ('https://gasdata.fluxys.com/lang/en',)
    base_url = 'https://gasdata.fluxys.com'
    DATE_FORMAT = '%d/%m/%Y'
    URL = 'https://gasdata.fluxys.com/sdp/Pages/Reports/Inventories.aspx?report=inventoriesLNG'

    def parse(self, response):
        return Request(self.URL, callback=self.parse_inventories)

    def parse_inventories(self, response):
        """Generate requests to excel files."""
        from_date_field = 'ctl00$ctl00$Content$ChildContentLeft$PeriodControl$PeriodFromDatePicker'
        from_date_value = self.start_date

        # Extract year by year to avoid "data volume is too high" error from server
        while from_date_value < self.today:
            formdata = {from_date_field: from_date_value.strftime(self.DATE_FORMAT)}
            yield FormRequest.from_response(
                response, formname='aspnetForm', formdata=formdata, callback=self.get_xls
            )
            from_date_value += ONE_YEAR

    # TODO: JM: we should use CSV export
    def get_xls(self, response):
        """Link to generate XLS is hide in a js script."""
        sel = Selector(response)
        res = sel.css('script').re('"ExportUrlBase":"(.*?)",')
        if res:
            res_str = self.base_url + res[0].replace('\\u0026', '&') + 'Excel'
            return Request(res_str, self.parse_xls)

        return None

    def parse_xls(self, response):
        obj_excel = ExcelExtractorZeebrugge(response.body, response.url, self.start_date)
        return obj_excel.parse_excel()
