from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.agents import ShipAgentMixin
from kp_scrapers.spiders.agents.kanoo_abudhabi import normalize_charters, normalize_grades
from kp_scrapers.spiders.bases.mail import MailSpider


class KanooLineupSpider(ShipAgentMixin, MailSpider):
    name = 'KN_AbuDhabi'
    provider = 'Kanoo'
    version = '1.0.0'

    # normalize data according to a schema
    normalize_data = None

    def parse_mail(self, mail):
        """Extract mail found with specified email filters in spider arguments.
        Args:
            mail (Mail):
        Yields:
            Dict[str, str]:
        """
        # get reported date from email subject
        reported_date = may_strip(mail.envelope['subject'].split('//')[-1])
        start_processing = False
        for tr in self.select_body_html(mail).xpath('//table//tr'):
            row = tr.xpath('.//td//span//text()').extract()
            if 'VESSELS' in row:
                start_processing = True
                header = row
                continue
            if start_processing and len(row) > 1:
                # get port name
                if row[0] != '\xa0' and row[1] == '\xa0':
                    current_port = row[0]
                # get raw information
                if len(row) == 8 and row[1] != '\xa0':
                    raw_item = {head: row[idx] for idx, head in enumerate(header)}
                    raw_item.update(
                        provider_name=self.provider,
                        reported_date=reported_date,
                        spider_name=self.name,
                        port_name=may_strip(current_port),
                    )

                    if DataTypes.SpotCharter in self.produces:
                        yield from normalize_charters.process_item(raw_item)
                    elif DataTypes.Cargo in self.produces:
                        yield from normalize_grades.process_item(raw_item)


class KanooChartersSpider(KanooLineupSpider):
    name = 'KN_AbuDhabi_Charters'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
        # prevent spider from marking the email as seen
        # so that multiple spiders can run on it
        'MARK_MAIL_AS_SEEN': False,
    }


class KanooGradesSpider(KanooLineupSpider):
    name = 'KN_AbuDhabi_Grades'
    produces = [DataTypes.CargoMovement, DataTypes.Vessel, DataTypes.Cargo]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': False,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }
