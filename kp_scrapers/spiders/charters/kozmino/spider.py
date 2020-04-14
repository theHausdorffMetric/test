import datetime as dt

from w3lib.html import remove_tags

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.kozmino import normalize, parser


class KozminoFixtures(CharterSpider, MailSpider):

    name = 'NC_Fixtures'
    provider = 'Maven Brokers'
    version = '1.0.0'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    port_name = 'Kozmino'

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        response = self.select_body_html(mail)
        # remove html tags
        # we do this instead of using xpath/css selectors since they will return newline breaks
        # which we don't want here
        raw_rows = [may_strip(remove_tags(row)) for row in response.xpath('//p').extract() if row]

        for idx, row in enumerate(raw_rows):
            # report has information from previous year, need to memoise the date
            # to normalize laycan, date checker should only be updated whens
            # appropriate row is matched i.e DECEMBER 2018
            try:
                date_checker = dt.datetime.strptime(row, '%B %Y')
            except:  # FIXME  # noqa
                pass

            row = row_correction(self, row)
            # break extraction once end of email reached
            if row == 'END':
                break

            # try and parse raw text into individual column elements
            row_elements, headers = parser.parse_raw_text(row)

            if row_elements:
                raw_item = {headers[idx]: cell for idx, cell in enumerate(row_elements)}
            else:
                continue

            # contextualise raw item with metadata
            raw_item.update(
                origin=self.port_name,
                provider_name=self.provider,
                reported_date=mail.envelope['date'],
                date_checker=date_checker,
            )

            yield normalize.process_item(raw_item)


def row_correction(self, input_row):
    """For rows without the unusual delimiter, certain charters with a space have be replaced.
    This would allow the vessel and charterer to be parsed correctly

    Args:
        input_row (str):

    Returns:
        String[str]

    Examples: # noqa
        >> parse_raw_text('EBN BATUTA CHEM CHINA 25-28 JAN 100,000MT P.DICKSON 480K')
        'EBN BATUTA CHEM.CHINA 25-28 JAN 100,000MT P.DICKSON 480K'

    """
    return input_row.replace('CHEM CHINA', 'CHEM.CHINA')
