import datetime
import os

import pandas
from scrapy import Request

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.datosgob_chile import normalize_grades
from kp_scrapers.spiders.agents.datosgob_chile.parser import (
    get_file_reported_date,
    get_import_page,
    get_months_in_year,
    get_rar_urls,
)
from kp_scrapers.spiders.bases.rar import RarSpider


class DatosBOLSpider(ShipAgentMixin, RarSpider):
    name = 'Datos'
    provider = 'DatosGob'
    version = '1.0.0'

    produces = []

    codes = ("2709", "271012", "2710190", "2710191", "2710192", "2710193", "2710194", "2710195")
    reported_date = datetime.datetime.now()
    number_of_past_dates = 2
    number_of_days = 30

    start_urls = ['http://datos.gob.cl/dataset']
    manifest_url = (
        'http://comext.aduana.cl:7001/ManifestacionMaritima/'
        'limpiarListaProgramacionNaves2.do;jsessionid=fxEf5FaH5m9GXC8gb03xqNsf '
    )

    def parse(self, response):
        # get year/months of targeted rar archives
        months_in_year = get_months_in_year(
            self.reported_date, self.number_of_past_dates, self.number_of_days
        )
        # download, extract and parse rar archives related to chosen years and months
        for year in months_in_year.keys():
            # get import page for a specific year
            import_page = get_import_page(self.start_urls[0], year)
            for month in months_in_year[year]:
                # get rar urls for a specific (year, month)
                rar_urls = get_rar_urls(import_page, month)
                if rar_urls:
                    self.logger.info(f'Start parsing file: ' + month + ' ' + year)
                    for document_path in self.extract_rar_io(rar_urls):
                        # get the file reported date
                        self.reported_date = get_file_reported_date(year, month)
                        # parse the file
                        yield from self.parse_document(document_path)
                        # delete extracted document
                        os.remove(document_path)
                else:
                    self.logger.warning(f'No downloaded file for ' + month + ' ' + year)

    def parse_document(self, document_path):
        # create a dataframe from the document path
        data = pandas.read_table(
            document_path, sep=";", header=None, dtype=str, keep_default_na=False
        )
        # filter dataframe regarding types of product
        filtered_data = data.loc[data[146].str.startswith(self.codes)]
        # build aw item for each row from the filtered dataframe
        for index, row in filtered_data.iterrows():
            manifest = row[44]
            if manifest:
                raw_item = {
                    'port_name': None,
                    'vessel_name': None,
                    'arrival': None,
                    'cargo_product': row[133]
                    + row[134]
                    + row[135]
                    + row[136]
                    + row[137]
                    + row[138]
                    + row[139],
                    'cargo_volume': row[150],
                    'cargo_movement': 'discharge',
                    'spider_name': self.name,
                    'provider_name': self.provider,
                    'reported_date': self.reported_date,
                }
                # send a request to the manifest site to get port/vessel information
                yield Request(
                    url=self.manifest_url,
                    body='%7BactionForm.programacion%7D=' + manifest,
                    method="POST",
                    headers={
                        'Authorization': 'Bearer whatever',
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                    callback=self.parse_response,
                    cb_kwargs=dict(raw_item=raw_item),
                )

    def parse_response(self, response, raw_item):
        # get port/vessel information
        raw_item['port_name'] = response.xpath(
            '//body//table[2]//tr[3]//td[2]//label//text()'
        ).extract_first()
        raw_item['vessel_name'] = response.xpath(
            '//body//table[2]//tr[3]//td[3]//label//text()'
        ).extract_first()
        raw_item['arrival'] = response.xpath(
            '//body//table[2]//tr[3]//td[9]//label//text()'
        ).extract_first()
        # create final yielded item
        if DataTypes.Cargo in self.produces:
            yield from normalize_grades.process_item(raw_item)


class DatosGradesSpider(DatosBOLSpider):
    name = 'Datos_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
