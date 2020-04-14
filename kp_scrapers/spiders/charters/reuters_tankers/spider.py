"""Module for Reuters tankers mail spider.

Scrape spot charters for clean and dirty products sent by Reuters on a weekly basis.
Parse the xls file attached in the email sent from Ahmad.Ghaddar@thomsonreuters.com.
This email typically contains energy markets insights and an attached xls with vessels and their
charterers.

A sample xls is available at:
tests/_fixtures/charters/reuters/MAY2018Clean.xlsx (clean products)
tests/_fixtures/charters/reuters/JUN2018Dirty.xlsx (dirty products)

"""
from dateutil.parser import parse as parse_date
from scrapy.exceptions import CloseSpider

from kp_scrapers.lib.xls import Workbook
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.reuters_tankers import normalize


class ReutersTankersSpider(CharterSpider, MailSpider):
    """Base ReutersTankers spider.

    Because ReutersEurope supplies both OIL and CPP charter reports with the same format,
    this spider will work for both emails.

    However, to allow for loading of jobs on separate platforms, subclasses of this class
    were created below.

    """

    name = 'RS_Tankers'
    version = '2.1.1'
    provider = 'ReutersEurope'

    # name of the first element of title row in xls workbook to scrap
    _first_title = 'charterer'

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract Kpler item from the given mail.

        Check if the email contains an xls file, if so, send the xls to the workbook scraper.

        Args:
            mail (Mail): see `lib.services.api.Mail` for details

        Yields:
            item (Dict[str, str]): Kpler item see process_item function for further type information

        Raises:
            CloseSpider: if no spreadsheet found in email

        """
        self.reported_date = parse_date(mail.envelope['date']).strftime('%d %b %Y')

        for attachment in mail.attachments():
            if attachment.is_spreadsheet:
                workbook = Workbook(content=attachment.body, first_title=self._first_title)
                for item in workbook.items:
                    # reported date might not be a proper date
                    try:
                        reported_date = parse_date(item['reported']).strftime('%d %b %Y')
                    except Exception:
                        reported_date = self.reported_date

                    item.update(
                        provider_name=self.provider, reported_date=reported_date, spider=self.name
                    )
                    yield normalize.process_item(item)
            else:
                self.logger.error('No spreadsheet found in email, closing spider!')
                raise CloseSpider('cancelled')


class ReutersDirtyTankersSpider(ReutersTankersSpider):
    """Hack to be able to separate spider jobs for OIL platform only.
    """

    name = 'RS_DirtyTankers'
    version = '2.1.2'
    provider = 'ReutersEurope'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }


class ReutersCleanTankersSpider(ReutersTankersSpider):
    """Hack to be able to separate spider jobs for CPP platform only.
    """

    name = 'RS_CleanTankers'
    version = '2.1.2'
    provider = 'ReutersEurope'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }


class ReutersWAFSpider(ReutersTankersSpider):
    """For WAF attachment
    """

    name = 'RS_WAF'
    version = '2.1.2'
    provider = 'ReutersWAF'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
