# -*- coding: utf8 -*-

from __future__ import absolute_import, unicode_literals
import collections
import csv
import datetime
from io import StringIO

from scrapy import FormRequest, Request
from scrapy.selector import Selector
import six

from kp_scrapers.models.items_inventory import IOItem
from kp_scrapers.spiders.bases.persist import PersistSpider

from . import OperatorSpider


PAYLOAD = {
    "tvDataItem_SelectedNode": "",
    "__EVENTTARGET": "lbtnCSVDaily",
    "__EVENTARGUMENT": "",
    "tvDataItem_PopulateLog": "13,58,",
    "txtSearch": "",
    "tvDataItemn181CheckBox": "on",  # Dragon D+1
    "tvDataItemn199CheckBox": "on",  # Grain NTS1, D+1
    "tvDataItemn201CheckBox": "on",  # Grain NTS2, D+1
    "tvDataItemn219CheckBox": "on",  # SouthHook, D+1
    "ctrlDateTime$rgApplicable": "rdoApplicableAt",
    "ctrlDateTime$txtSpecifyFromDate": "",
    "ctrlDateTime$txtSpecifyToDate": "",
    "hdnIsAddToList": "",
}

INSTALLATIONS = {
    'Dragon': 'Dragon',
    'GrainNTS1': 'Grain',
    'GrainNTS2': 'Grain',
    'SouthHook': 'SouthHook',
}


def _get_installation(name):
    for each in INSTALLATIONS.keys():
        if each in name:
            return INSTALLATIONS[each]


def group_items(items):
    results = collections.OrderedDict()
    for item in items:
        date = datetime.datetime.strptime(item['Applicable For'], "%d/%m/%Y")
        installation = _get_installation(item['Data Item'])
        if not installation:
            continue
        results.setdefault(date, {}).setdefault(installation, 0)
        results[date][installation] += float(item['Value'])
    return results


class UKOperatorHistoricalSendOutSpider(OperatorSpider, PersistSpider):

    name = 'UKOperatorHistoricalSendOut'
    URL = 'http://marketinformation.natgrid.co.uk/gas/frmDataItemExplorer.aspx'
    allowed_domains = ['marketinformation.natgrid.co.uk']

    def parse_csv(self, str_csv):
        self.logger.debug("Parsing csv body")
        f = StringIO.StringIO(str_csv)
        reader = csv.DictReader(f, delimiter=str(','))
        values = list(reader)
        grouped_items = group_items(values)
        for date, val in six.iteritems(grouped_items):
            for k, v in six.iteritems(val):
                self.logger.info('{} {} {}'.format(date, k, v))
                yield IOItem(date=str(date), city=k, output_o=str(v))

    def start_requests(self):
        yield Request(self.URL, callback=self.parse_request)

    def parse_request(self, response):
        sel = Selector(response)
        error_check = sel.xpath('//text()').extract()

        for text in error_check:
            if 'application is currently unavailable' in text:
                self.logger.warning('Webpage is unavailable for the moment')
                return

        formdata = PAYLOAD
        today = datetime.datetime.now().date()
        from_date = self.start_date
        # Dynamic forms
        formdata['ctrlDateTime$txtSpecifyFromDate'] = from_date.strftime('%d/%m/%Y 00:00')
        formdata['ctrlDateTime$txtSpecifyToDate'] = today.strftime('%d/%m/%Y 00:00')
        formdata['tvDataItem_ExpandState'] = sel.xpath(
            '//input[@id="tvDataItem_ExpandState"]/@value'
        ).extract_first()
        formdata['__VIEWSTATE'] = sel.xpath('//input[@id="__VIEWSTATE"]/@value').extract_first()
        formdata['__EVENTVALIDATION'] = sel.xpath(
            '//input[@id="__EVENTVALIDATION"]/@value'
        ).extract_first()
        formdata['__VIEWSTATEGENERATOR'] = sel.xpath(
            '//input[@id="__VIEWSTATEGENERATOR"]/@value'
        ).extract_first()
        self.logger.debug('start sending form request')

        yield FormRequest(self.URL, formdata=formdata, callback=self.parse)

    def parse(self, response):
        for item in self.parse_csv(response.body):
            yield item
