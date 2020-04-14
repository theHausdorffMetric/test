from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.pdf import PdfSpider
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.tampico import normalize


def append_row_event(table):
    """Append section number to the end of the row.

    The pdf contains multiple table sections.
    Each section lists the lineup of vessels of a specific event type.
    Therefore, we need to include this implicit info in the row itself.

    Args:
        table (List[List[str]]):

    Yields:
        row (List[str]):

    """
    section_no = 0
    for row in table:
        row = [may_strip(cell) for cell in row]
        # It checks if the row is a title line and it changes the section number if it is.
        title = (row[5] + row[6] + row[7] + row[8] + row[9]).replace(' ', '')
        if title == 'BUQUESPROXIMOSAARRIBAR':
            section_no = 1
        elif title == 'BUQUESFONDEADOS':
            section_no = 2
        elif title == 'BUQUESZARPADOS':
            section_no = 3

        # append section info to the row, so we know what event it is
        row.append(section_no)
        yield row


class TampicoSpider(PortAuthoritySpider, PdfSpider):
    name = 'Tampico'
    provider = 'Tampico'
    version = '1.0.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = ['http://consultarf.puertodetampico.com.mx/buques/rpProgBuq.pdf']

    tabula_options = {
        '-p': ['all'],
        '-a': ['45.9,10.328,729.81,581.018'],
        '-c': ['13.75,55,123,146,167,204,241,279,307.9,367.3,395.3,446.1,519.7,572'],
    }

    def parse(self, response):
        """Extract data from pdfs.

        There are 3 main loops here, with each loop represent a section in the pdf.
        `row[-1]` is the section number (begins at 0), used as a breakpoint.
        The alignment of columns isn't the same between sections.
        Hence, tabula options will need to be changed between breakpoints.

        Args:
            response (scrapy.Response):

        Yields:
            Dict[str, str]:

        """
        table = list(
            filter(None, self.extract_pdf_table(response, append_row_event, **self.tabula_options))
        )

        reported_date = self.extract_reported_date(table)
        breakpoint = 0
        for idx, row in enumerate(table):
            if row[-1] == 1:
                breakpoint = idx
                break
            else:
                yield normalize.process_item(row=row, reported_date=reported_date, url=response.url)

        self.tabula_options['-c'] = ['13.75,50,120,149,193,228,274,311,395,423,504,573']
        table = list(
            filter(None, self.extract_pdf_table(response, append_row_event, **self.tabula_options))
        )
        for row in table[breakpoint:]:
            if row[-1] == 2:
                break
            else:
                yield normalize.process_item(row=row, reported_date=reported_date, url=response.url)

        self.tabula_options['-c'] = ['10.75,50,120,158,194,244,292,395,420,500,573']
        table = list(
            filter(None, self.extract_pdf_table(response, append_row_event, **self.tabula_options))
        )
        for row in table[breakpoint:]:
            if row[-1] == 3:
                break
            else:
                yield normalize.process_item(row=row, reported_date=reported_date, url=response.url)

    def extract_reported_date(self, table):
        """Extract reported date from the table.

        Reported date will always be contained in the first line.
        We retreive the list.
        Concatenate the elements (except for the last element; the section number appended earlier)

        Args:
            table (List[List[str]]):

        Returns:
            str: raw reported date

        """
        return ''.join(table[0][:-1]).replace(' ', '')
