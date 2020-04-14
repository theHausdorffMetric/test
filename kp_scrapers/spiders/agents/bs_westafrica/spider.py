from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.bs_westafrica import normalize
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.bases.pdf import PdfSpider


# name of pdf to extract data from
PDF_NAME = 'west africa product tankers'


class BSWestAfricaSpider(ShipAgentMixin, PdfSpider, MailSpider):
    name = 'BS_WestAfricaProductTankers'
    provider = 'Blueseas'
    version = '1.0.0'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    # memoise reported date so it won't need to be called repeatedly
    reported_date = None

    @staticmethod
    def pdf_extraction_options(page):
        """Build an options dict for Tabula to use.

        """
        return {'--pages': [str(page)], '--stream': [], '--area': ['0,0,100,700']}

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:

        """
        for attachment in mail.attachments():
            # sanity check, in case we receive irrelevant files
            if PDF_NAME not in attachment.name.lower() or not attachment.is_pdf:
                continue

            # extract reported date for use later
            self.reported_date = self._extract_reported_date(attachment.name)

            yield from self.parse_pdf_table(attachment)

    def parse_pdf_table(self, attachment):
        """Extract raw data from PDF attachment in email.

        Tabula, in lattice mode, is unable to detect titles (containing port name)
        when they exist at top of the page. Hence, we go over each page and use
        1) stream mode + define the top title area to extract top titles, and
        2) lattice mode to extract the table rows normally.

        PDF is structured such that extracted data will look something like this:  # noqa
        13 ['LAGOS: SINGLE BUOY MOORING (SBM)', '', '', '', '', '', '', '', '']
        14 ['POSITION', 'SHIP’S NAME', 'CARGO', 'QTY\r[MT]', 'ARRVD\r[ETA]', 'ETB', 'SAILED\r[ETD]', 'CHARTERERS/\rRECEIVERS', 'REMARKS']
        15 ['VACANT', '-', '-', '-', '-', '-', '-', '-', 'Vacant, Torm Eric sailed AM/ 28']
        16 ['LAGOS: NEW ATLAS COVE JETTY (NACJ) Lat 060 24.77N Long. 0030 23.88E', '', '', '', '', '', '', '', '']
        17 ['POSITION', 'SHIP’S NAME', 'CARGO', 'QTY\r[MT]', 'ARRVD\r[ETA]', 'ETB', 'SAILED\r[ETD]', 'CHARTERERS/\rRECEIVERS', 'REMARKS']
        18 ['AT BERTH', 'MSK TANGIER', 'PMS', '38,024', '10 MAY-18', '29 MAY-18', '03 JUNE-18', 'BP OIL/ PPMC', 'Awtg clearances to commence discharge\rPM/ 30']

        Args:
            attachment (Attachment):

        Yields:
            Dict[str, str]:

        """
        # iterate across all pages and extract top port name
        page = 1
        while True:

            port_row = None
            # we skip the first page as it does not contain top titles
            if page > 1:
                # extract the top title (port name) in stream and area mode
                port_row = self._extract_top_title(attachment, page)
                if not port_row:
                    # all pages are already parsed, hence, we stop the loop
                    break

            # extract the table values in lattice mode
            table = self.extract_pdf_io(attachment.body, **{'-p': [str(page)], '-l': []})
            if port_row:
                table.insert(0, port_row)

            for row in table:
                # extract table headers
                if 'POSITION' in row[0]:
                    header = row
                    continue

                # skip filler rows
                if len(row) < 2:
                    continue

                # skip VACANT vessels (quick win to ignore such rows)
                # pdf extraction is a bit indeterministic and may provide special hyphen chars
                if row[0] in ['', 'VACANT'] or row[1] in ['\u002d', '\u2010', '\u2011', '\u2012']:
                    continue

                # memoise port name from sub-table header row
                if row[0] and not any(row[1:]):
                    port_name = row[0]
                    continue

                # transform row to a mapped raw dict
                if len(header) == len(row):
                    raw_item = {head: row[idx] for idx, head in enumerate(header)}
                    raw_item.update(
                        port_name=port_name,
                        provider_name=self.provider,
                        reported_date=self.reported_date,
                    )
                    yield from normalize.process_item(raw_item)

            # increment page number so that we can extract data from succeeding pages
            page += 1

    def _extract_top_title(self, attachment, page):
        # extract the top title (port name) in stream and area mode
        try:
            return self.extract_pdf_io(
                attachment.body,
                **{
                    '--pages': [str(page)],
                    '--stream': [],
                    # area which contains top title (port name) on a page is
                    # vertical end is slightly extended to take table columns into account
                    '--area': ['0,0,100,700'],
                },
            )[0]
        except IndexError:
            # page number is not present within the report
            return

    @staticmethod
    def _extract_reported_date(date_str):
        for str_to_remove in ['west africa product tankers report', '.pdf']:
            date_str = date_str.lower().replace(str_to_remove, '')

        return date_str
