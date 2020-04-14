# -*- coding: utf-8 -*-
"""Scrape daily sendin of two pipelines of Sabin Pass operator.

# Abstract
The two Sabin Pass pipelines scraped are NGPL and KMLP.
The spider yield two items per day.
Data is available from 2017-01-15 on the website.

# How to check data manually on the website:
## KMLP
- Goto http://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=KMLP
- Click on retrieve button
- Look for 'SABPL/KMLP'

## NGPL
- Goto http://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=NGPL
- Click on retrieve button
- On table header click on fiter icon for column 'Loc name'
- Filter on 'SPLIQ/NGPL'
- Look for 'SPLIQ/NGPL'

# How the spider works
The spider mimic the manual process.
Form data is copy pasted from browser developer tool.
One request will be made for each pipeline and date requested.

# Limitation
We cannot scrape historical data.
One can try to gather historical data with the folowing formdata:
> DATE_FORMDATA_KEY = 'ctl00_WebSplitter1_tmpl1_ContentPlaceHolder1_dtePickerBegin_clientState'
> DATE_FORMDATA_VALUE = '|0|01{date}-0-0-0-0||[[[[]],[],[]],[{{}},[]],"01{date}-0-0-0-0"]'
> EXPECTED_DATE_FORMAT = '%Y-%m%-d'
But it doesn't work currently.
"""
from __future__ import absolute_import, unicode_literals
import datetime as dt

from scrapy import Spider
from scrapy.http import FormRequest, Request

from kp_scrapers.models.items import USAModuleSendIn
from kp_scrapers.spiders.operators import OperatorSpider


BASE_URL = 'https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code={pipeline_shortname}'

MAP_PIPELINE_SHORTNAME_FULLNAME = {
    'NGPL': 'SPLIQ/NGPL SABINE PASS LIQUEFACTION',
    'KMLP': 'SABPL/KMLP  CALCASIEU',
}

NGPL_FORMDATA = {
    'WebSplitter1_tmpl1_ContentPlaceHolder1_DGOpAvail_clientState': '[[[[null,75,0,0]],[[[[[0]],[],null],[null,null],[null]],[[[[7]],[],null],[null,null],[null]],[[[["ColumnMoving"]],[],[{}]],[{},[{}]],null],[[[["ColumnResizing",null]],[],[{}]],[{},[{}]],null],[[[["Filtering",null]],[[[[[null,null,null]],[],[]],[{},[]],null]],[{}]],[{},[{}]],null],[[[["Paging"]],[],[]],[{},[]],null],[[[["Sorting"]],[],[{"View":[[0]],"all_quan_avail":[[0]]}]],[{},[{}]],null],[[[["Selection",null,null,1,null,null,null]],[],[]],[{},[]],null],[[[["ColumnFixing",null]],[[[[[]],[],null],[null,null],[null]],[[[[]],[],null],[null,null],[null]]],[{}]],[{},[{}]],null]],null],[{},[{},{}]],[{"ownerName":"Filtering","type":"ApplyFilter","id":null,"value":[{"_columnKey":"PT_ID_NBR","_columnType":"number","_gridID":"WebSplitter1_tmpl1_ContentPlaceHolder1_DGOpAvail","_condition":{"_type":"number","_rule":1,"_value":46622,"_colKey":"PT_ID_NBR","_gridID":"WebSplitter1_tmpl1_ContentPlaceHolder1_DGOpAvail"}}],"tag":28800000}]]',  # noqa
    'WebSplitter1_tmpl1_ContentPlaceHolder1_DGOpAvail_x__ig_def_ep_n': '46622',
}

ROW_XPATH = '//td[contains(text(), "{pipeline_fullname}")]//parent::tr//td[8]//text()'
DATE_XPATH = '//input[@data-ig="x:473969352.0:mkr:3"]/@value'
DATE_FORMAT = '%m/%d/%Y'


class USASabinePassLouisiannaNGPL(OperatorSpider, Spider):

    name = 'SabinePassKMLP_NGPL'
    provider = 'SabinePassKMLP_NGPL'
    version = '1.0.0'
    allowed_domains = ['pipeline2.kindermorgan.com']
    installation_id = 217
    unit = 'MMBTU'

    @staticmethod
    def _init_form_data(pipeline):
        return NGPL_FORMDATA if pipeline == 'NGPL' else {}

    def start_requests(self):
        for pipeline in MAP_PIPELINE_SHORTNAME_FULLNAME.keys():
            yield Request(
                BASE_URL.format(pipeline_shortname=pipeline),
                callback=self.fetch_page,
                meta={'pipeline': pipeline},
            )

    def fetch_page(self, response):
        pipeline = response.meta['pipeline']
        formdata = self._init_form_data(pipeline)
        yield FormRequest.from_response(
            response, formdata=formdata, callback=self.parse_page, meta={'pipeline': pipeline}
        )

    def parse_page(self, response):
        # Website breaks for random raison
        if 'An Error Has Occurred' in response.body.decode('utf-8'):
            self.logger.error('Page unvailable')
            return

        pipeline = response.meta['pipeline']
        row_xpath = ROW_XPATH.format(pipeline_fullname=MAP_PIPELINE_SHORTNAME_FULLNAME[pipeline])
        raw_value = response.xpath(row_xpath).extract_first()
        raw_date = response.xpath(DATE_XPATH).extract_first()

        # We do not want to yield item without value or date
        # If we can't parse them, it means the website changed.
        try:
            value = int(raw_value.replace(',', ''))  # US number
        except (AttributeError, ValueError) as exc:
            raise type(exc)('Could not find sendin for Sabin Pass {} pipeline'.format(pipeline))
        try:
            date = dt.datetime.strptime(raw_date, DATE_FORMAT).isoformat()
        except ValueError:
            raise ValueError('Could not find date for Sabin Pass {} pipeline'.format(pipeline))

        item = USAModuleSendIn()
        item['installation_id'] = self.installation_id  # Sabine Pass (Liqu.)
        item['unit'] = self.unit
        item['date'] = date
        item['pipeline'] = pipeline
        item['url'] = response.url
        item['value'] = value

        yield item
