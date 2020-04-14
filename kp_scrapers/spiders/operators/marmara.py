# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals

from scrapy.selector import Selector
from scrapy.spiders import Spider

from kp_scrapers.models.items_inventory import IOItem

from . import OperatorSpider


class MaramaraOperatorSpider(OperatorSpider, Spider):

    name = "MarmaraOperator"
    allowed_domains = ["lngebt.botas.gov.tr"]
    start_urls = ("http://lngebt.botas.gov.tr/OfflineReport.aspx?newsid=N000000019",)

    output_total = 0.0

    def get_float(self, row, pos):
        content = row.css("td:nth-child(" + str(pos) + ")::text").extract()[0]
        try:
            self.logger.debug("content == {}".format(content))
            return float(content.replace('.', ''))
        except ValueError:
            return 0

    def default_item(self, item, response):
        item["unit"] = "M3"
        item['city'] = "marmara"
        item['src_file'] = response.url

    def parse(self, response):
        sel = Selector(response)
        tr_rows = sel.css("table tr")
        item = IOItem()
        for tr_row in tr_rows:
            css_ex = tr_row.css("td:nth-child(1)::text")
            if not css_ex:
                continue
            css_txt = css_ex[0]
            if css_txt.re("D.NEM\sSONU\sSTOK"):
                item["level_o"] = self.get_float(tr_row, 3)
            elif (
                css_txt.re("SEVK\sED.LEN\sGAZ")
                or css_txt.re("DAH.L.\sT.KET.M")
                or css_txt.re("SANAYI\sM...SATILAN")
            ):
                self.output_total += self.get_float(tr_row, 3)
                if css_txt.re("SEVK\sED.LEN\sGAZ"):
                    item['output_o'] = self.get_float(tr_row, 3)
            elif css_txt.re(".THAL\sED.LEN\sGAZ"):
                item["input_cargo"] = self.get_float(tr_row, 3)
        item["outflow_o"] = self.output_total

        # Set date
        date_re = sel.css(".Title1::text").re("([0-9]{2})\.([0-9]{2})\.([0-9]{4})")
        if date_re:
            date_str = "%s-%s-%s" % (date_re[2], date_re[1], date_re[0])
            item["date"] = date_str

        # Fill last item fields
        self.default_item(item, response)
        return item
