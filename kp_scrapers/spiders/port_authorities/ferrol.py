# -*- coding: utf-8 -*-

"""Ferrol

This spider crawls the site of the port authority of Ferrol.
The document parsed is a pdf.

Website: http://www.apfsc.es/sid/usuarios/login

"""

from __future__ import absolute_import, unicode_literals
from datetime import datetime
import glob
import os
import re

from scrapy.http import FormRequest, Request
from scrapy.selector import Selector
from six.moves import range

from kp_scrapers.lib.pdf import ErosionPdfTable
from kp_scrapers.models.items import VesselPortCall
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


LOGIN = 'tfranco@reganosa.com'
PASSWORD = 'ZZ12aa'

USELESS_URL = 'http://www.yahoo.com/'

_COMMODITIES_MAP = ['LNG', 'LPG', 'GAS NATURAL', 'CRUDE']
_AUTORIZADAS_COLUMNS = [
    # ? (query to port authorities under way)
    ('enum+S+F', None, 'Atra. Esc. Subp.'),
    # Year of the event
    ('date+%Y', None, 'Atra. Esc. Año'),
    # Internal serial number (unimportant)
    ('int', None, 'Atra. Esc. Núm'),
    # Ship name or number
    ('str', 'vessel_name', 'Atra. Esc. Buque Nombre Buque'),
    # Operation D: unload, E: load, REPARACION: anchorage, E. TECNICA: ?
    ('enum+E+D+REPARACION+E. TECNICA+ESCALA TECNICA+E.TECNICA+PC+T', 'event', 'T. Oper. Cód.'),
    # The comodity loaded/unload
    ('str', 'cargo_type', 'Merc.'),
    # Comodity amount normalized in tons
    ('float*', 'cargo_ton', 'Cant. Mov.'),
    # Berth name
    ('str*', 'berth', 'Atra. Aut. Alin. Desc.'),
    # ? (first mooring post)
    ('float*', None, 'Atra. Aut. Noray Ini.'),
    # ? (last mooring post)
    ('float*', None, 'Atra. Aut. Noray Fin.'),
    # ETA
    (
        'date+%d-%m-%Y '
        '%H:%M+%d.%m.%y '
        '%H:%M+%d.%m.%Y '
        '%H:%M+%d.%m.%Y '
        '%H0%M+%d.%m.%Y '
        '%H:%M:%S+%d.%m.%Y '
        '%I:%M:%S',
        'eta',
        'Atra. Aut. Fec. Ini.',
    ),
]

_SOLICITADOS_COLUMNS = [
    # ? (query to port authorities under way)
    ('enum+S+F', None, 'Atra. Esc. Subp.'),
    # Year of the event
    ('date+%Y', None, 'Atra. Esc. Año'),
    # Internal serial number (unimportant)
    ('int', None, 'Atra. Esc. Núm'),
    # Ship name or number
    ('str', 'vessel_name', 'Atra. Esc. Buque Nombre Buque'),
    # Operation
    ('enum+E+D+REPARACION+E. TECNICA+ESCALA TECNICA+E.TECNICA+PC+T', 'event', 'T. Oper. Cód.'),
    # The comodity loaded/unload
    ('str', 'cargo_type', 'Merc.'),
    # Comodity amount (in tons)
    ('float', 'cargo_ton', 'Cant. Mov.'),
    ('str', 'berth', 'Atra. Sol. Alin. Desc.'),
    ('float*', None, 'Atra. Sol. Noray Ini.'),
    ('float*', None, 'Atra. Sol. Noray Fin.'),
    # ETA
    (
        'date+%d-%m-%Y '
        '%H:%M+%d.%m.%y '
        '%H:%M+%d.%m.%Y '
        '%H:%M+%d.%m.%Y '
        '%H0%M+%d.%m.%Y '
        '%H:%M:%S+%d.%m.%Y '
        '%I:%M:%S',
        'eta',
        'Atra. Sol. Fec. Ini.',
    ),
]

_INICIADAS_COLUMNS = [
    # ? (query to port authorities under way)
    ('enum+S+F', None, 'Atra. Esc. Subp.'),
    # Year of the event
    ('date+%Y', None, 'Atra. Esc. Año'),
    # Internal serial number (unimportant)
    ('int', None, 'Atra. Esc. Núm'),
    # Ship name or number
    ('str', 'vessel_name', 'Atra. Esc. Buque Nombre Buque'),
    # Operation
    ('enum+E+D+REPARACION+E. TECNICA+ESCALA TECNICA+E.TECNICA+PC+T', 'event', 'T. Oper. Cód.'),
    ('str', 'cargo_type', 'Merc.'),
    ('float', 'cargo_ton', 'Cant. Mov.'),
    ('str', 'berth', 'Atra. Real Alin. Desc.'),
    ('float*', None, 'Atra. Real Noray Ini.'),
    ('float*', None, 'Atra. Real Noray Fin.'),
    (
        'date+%d-%m-%Y '
        '%H:%M+%d.%m.%y '
        '%H:%M+%d.%m.%Y '
        '%H:%M+%d.%m.%Y '
        '%H0%M+%d.%m.%Y '
        '%H:%M+%d.%m.%Y '
        '%H:%M:%S+%d.%m.%Y '
        '%I:%M:%S',
        'arrival_date',
        'Atra. Real Fec. Ini.',
    ),
    # previous country
    ('str*', None, 'Atra. Esc. País Ant. Desc.'),
    # etd
    ('str*', 'departure_destination', 'Atra. Esc. País Post. Desc.'),
]

_FINALIZADOS_COLUMNS = [
    # ? (query to port authorities under way)
    ('enum+S+F', None, 'Atra. Esc. Subp.'),
    # Year of the event
    ('date+%Y', None, 'Atra. Esc. Año'),
    # Internal serial number (unimportant)
    ('int', None, 'Atra. Esc. Núm'),
    # Ship name or number
    ('str', 'vessel_name', 'Atra. Esc. Buque Nombre Buque'),
    # Operation
    ('enum+E+D+REPARACION+E. TECNICA+ESCALA TECNICA+E.TECNICA+PC+T', 'event', 'T. Oper. Cód.'),
    # Comodity loaded/unloaded
    ('str', 'cargo_type', 'Merc.'),
    # Comodity amount (in tons)
    ('float', 'cargo_ton', 'Cant. Mov.'),
    # Berth name
    ('str', 'berth', 'Atra. Real Alin. Desc.'),
    ('float*', None, 'Atra. Real Noray Ini.'),  # ? First mooring post used
    ('float*', None, 'Atra. Real Noray Fin.'),  # ? Last mooring post used
    # Arrival date
    (
        'date+%d-%m-%Y %H:%M+%d.%m.%y %H:%M+%d.%m.%Y %H:%M+%d.%m.%Y %H:%M:%S',
        'arrival_date',
        'Atra. Real Fec. Ini.',
    ),
    # Departure date
    (
        'date+%d-%m-%Y '
        '%H:%M+%d.%m.%y '
        '%H:%M+%d.%m.%Y '
        '%H:%M+%d.%m.%Y '
        '%H0%M+%d.%m.%Y '
        '%H:%M:%S+%d.%m.%Y '
        '%I:%M:%S',
        'departure_date',
        'Atra. Real Fec. Fin.',
    ),
]


class FerrolTable(ErosionPdfTable):
    _START = 'right'
    _OPERATIONS = ('E', 'D', 'REPARACION', 'E.TECNICA')
    _TITLES = [
        'AUTORIZADAS',  # Mind
        'ATORIZADAS',  # Mind the typo it is found in some documents
        'FINALIZADOS',
        'INICIADAS',
        'SOICITADAS',  # Mind the typo it is found in some documents
        'SOLICITADOS',
    ]
    _HEADER_STOP = [
        'Atra. Real Fec. Fin.',  # End of port call (initiated port calls)
        'Atra. Esc. País Post. Desc.',  # Next port (finalized port calls)
        'Atra. Sol. Fec. Ini.',  # End of port call (tentative port calls)
        'Atra. Aut. Fec. Ini.',  # ETA (authorized port calls)
    ]

    def __init__(self, content, filename, logger):
        name = os.path.basename(filename)
        is_autorizad = name.startswith('AUT')
        is_solicitad = name.startswith('SOL') or name.startswith('SOI')
        is_finalizad = name.startswith('FIN')
        is_iniciad = name.startswith('INI')
        if is_autorizad:
            self._COLUMNS = _AUTORIZADAS_COLUMNS
        elif is_solicitad:
            self._COLUMNS = _SOLICITADOS_COLUMNS
        elif is_finalizad:
            self._COLUMNS = _FINALIZADOS_COLUMNS
        elif is_iniciad:
            self._COLUMNS = _INICIADAS_COLUMNS
            self._START = 'left'
        else:
            raise ValueError('Could not guess file content from its name {}.'.format(filename))

        ErosionPdfTable.__init__(self, content, filename, logger)

    def get_columns(self):
        """Returns different column sets depending on the lines content.

        This simplifies the search, and renders it more successful.

        If the line contains the word REPARACION, we remove the 'cargo_type'
        ('Merc.) and 'cargo_tons' ('Cant. Mov.') columns, which are always
        empty.

        If the line contains CRUCERISTAS or PASAJEROS it concerns the arrival
        of a cruise ship, which we do not care about so we return an empty set
        of column.

        """
        if 'REPARACION' in self._line or 'TECNICA' in self._line:
            idx = [x[2] for x in self._COLUMNS].index('Merc.')
            return self._COLUMNS[:idx] + self._COLUMNS[idx + 2 :]
        elif 'CRUCERISTAS' in self._line or 'PASAJEROS' in self._line:  # Trick to skip cruise ships
            return []
        return super(FerrolTable, self).get_columns()

    def get_strategy(self):
        if _INICIADAS_COLUMNS == self._COLUMNS:
            return [('left', x) for x in range(0, len(self._COLUMNS))] + [
                ('right', x) for x in range(0, 0)
            ]
        return None


class FerrolSpider(PortAuthoritySpider, PdfSpider):

    # Duplicated with etl/extraction/port_authorities/ferrol_pa.py
    name = 'Ferrol'
    short_name = 'Ferrol'
    allowed_domains = ['apfsc.es']
    start_urls = ('http://www.apfsc.es/sid/usuarios/login',)

    def __init__(self, parseonly=False, *arg, **kwargs):
        super(FerrolSpider, self).__init__(*arg, **kwargs)
        self.parseonly = bool(parseonly)

    def start_requests(self):
        if self.parseonly is False:
            url = self.start_urls[0]
            formdata = {'data[Usuario][usuario]': LOGIN, 'data[Usuario][clave]': PASSWORD}
            yield FormRequest(url, formdata=formdata, callback=self.navigate)
        else:
            # Skip downloading files, simple parse the PDF files found
            # in the spider's ``data_path`` directory.
            yield Request(url=USELESS_URL, callback=self.parse_directory)

    def parse_directory(self, _):
        """Parses the files found in the spider's ``data_path`` directory

        """
        for pdffile in os.listdir(self.data_path):
            with open(os.path.join(self.data_path, pdffile), 'r') as pdf_reader:
                for item in self.parse_file(pdffile, pdf_reader.read()):
                    yield item

    def _field(self, table, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(table.css(q).extract()).strip()

    def navigate(self, response):
        selector = Selector(response)
        for a in selector.css('.listadoficheros td.pdf a'):
            if 'pdf' in ''.join(a.css('::text').extract()).lower():
                url = a.css('::attr(href)').extract()[0]
                yield Request(url, callback=self.parse_pdf)

        for a in selector.css('.listadoficheros td.dir a'):
            if 'buques' in ''.join(a.css('::text').extract()).lower():
                url = a.css('::attr(href)').extract()[0]
                yield Request(url, callback=self.navigate)

    def parse_pdf(self, response):
        content = response.body

        filename = re.search('filename="(.*)"', response.headers['Content-Disposition']).groups()[0]
        if 'plano' not in filename.lower():
            self.save_file(filename, content)

            if self.auto_parse:
                for item in self.parse_file(filename, content):
                    yield item

    @classmethod
    def to_anchorage(cls, port_call):
        event = port_call.get('event')
        if event is None:
            return False

        return event.endswith('tecnica') or event == 'reparacion'

    @classmethod
    def accept_cargo_type(cls, port_call):
        cargo_type = port_call.get('cargo_type')
        if cargo_type in _COMMODITIES_MAP:
            return cargo_type.lower()

    def tabula(self, pdf_file, path_pdf, pdf2text):
        os.chdir(path_pdf)
        os.system(
            "tabula -g -r "
            + pdf_file.replace(' ', '\ ')
            + " -p all -f TSV "
            + " -o "
            + pdf_file.replace('.pdf', '').replace(' ', '\ ')
            + ".tsv"
        )
        tsv_name = glob.glob(pdf_file.replace('.pdf', '') + '.tsv')[0]

        with open(str(tsv_name), 'r') as tsv:
            tsv_file = [j.decode('utf-8').replace('\r\n', '').split('\t') for j in tsv]
        for line in tsv_file:
            for element in line:
                # the spreadsheet algorithm of tabula lost few characters at
                # his output. To avoid this, I compare the result of pdftotext
                # and the output of tabula. I replace the element of pdftotext
                # that seems equivalent at each element of this output.
                match = re.search(r'\w*' + element + r'\w*', pdf2text)
                if match:
                    line[line.index(element)] = match.group()

        return tsv_file

    def parse_date(self, date_to_parse):

        date_to_parse = datetime.strptime(date_to_parse, "%d-%m-%Y %H:%M")
        return date_to_parse

    @classmethod
    def parse_file(cls, filename, content):
        updated_time = re.match('\D*(\d.*).pdf', filename).groups()[0]
        m = re.match('(\d{2}).(\d{2}).(\d{2,4})', updated_time)
        if m:
            updated_time = '/'.join(m.groups())

        # Raises RuntimeError if cannot find the header or end of the table.
        # Raises ValueError if cannot guess expected content from the filename.
        port_calls = FerrolTable(
            cls.pdf_to_text(content),
            os.path.basename(filename),
            # The get_logger function allows to get a logger without
            # instanciating the class.
            cls.get_logger(),
        ).parse()
        for port_call in port_calls:
            repair = cls.to_anchorage(port_call)
            if cls.accept_cargo_type(port_call) or repair:
                port_call.update({'url': cls.start_urls[0], 'updated_time': updated_time})
                if repair:
                    port_call['position_in_port'] = port_call['event']

                if 'event' in port_call:
                    del port_call['event']

                item = VesselPortCall(**port_call)

                yield item

                if item.get('departure_destination'):
                    next_item = VesselPortCall()
                    next_item['foreign'] = True
                    next_item['missing_eta'] = True
                    next_item['vessel_name'] = item.get('vessel_name')
                    # next_item['vessel_flag'] = item.get('vessel_flag')
                    next_item['port_name'] = item.get('departure_destination')
                    next_item['origin_etd'] = item.get('departure_date') or item.get('etd')
                    next_item['url'] = item.get('url')

                    yield next_item
