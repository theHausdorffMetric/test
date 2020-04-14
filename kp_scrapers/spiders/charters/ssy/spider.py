import re

from kp_scrapers.lib.parser import may_strip
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.mail import MailSpider
from kp_scrapers.spiders.charters import CharterSpider
from kp_scrapers.spiders.charters.ssy import normalize


MISSING_ROWS = []


class SimpsonSpenceYoungSpider(CharterSpider, MailSpider):

    name = 'SSY_EOS_LR1LR2'
    provider = 'SSY'
    version = '0.2.1'
    produces = [DataTypes.SpotCharter]

    spider_settings = {
        # push items on GDrive spreadsheet
        'KP_DRIVE_ENABLED': True,
        # notify in Slack the document is ready
        'NOTIFY_ENABLED': True,
    }

    def parse_mail(self, mail):
        """Extract data from mail specified with filters in spider args.

        As of 24th July 2019, this is the format as sent via email (as an example):

        ```
        LR1:
        =========================================================================================
        CARGO
        ATC                90KT ULSD       AG/UKC-SPORE-EAFR            04-07 AUG - WDWF
        YNCC               75KT NAP        AG/JAPAN                        10 AUG
        -
        SUBS
        SWARNA KAMAL      ON SUBS  60KT GO+JET  KWT/DJIBOUTI            26-27 JUL   450K      KPC
        SCF PLYMOUTH      ON SUBS  55KT NAP     SIKKA/JAPAN             02-04 AUG    W98.5    EXXON
        ALTESSE           ON SUBS  60KT CPP     VADINAR/SING            03-05 AUG    W100     VITOL
        JO ROWAN          ON SUBS  60KT HSD     N.MANG/EAFR-SPORE       28-30 JUL    O/P      TRAFI
        MARIANN           ON SUBS  FUJAIRAH     17 JUL - ATC COA.
        HAFNIA AUSTRALIA  ON SUBS  KARACHI      19 JUL - ATC COA.
        -
        FLD
        BREEZY VICTORIA   FAILED   60KT UMS     JEBEL ALI/PAKI          25-27 JUL   270K      ENOC
        -
        FXD
        HAFNIA HONG KONG  FIXED    60KT JET     AG/WEST                 29-30 JUL   1.675M    CHEVR
        ```

        Args:
            mail (Mail):

        Yields:
            Dict[str, str]:
        """
        do_process = False

        raw_rows = self.select_body_html(mail).xpath('//div')

        for idx, raw_row in enumerate(raw_rows):
            raw_row = raw_row.xpath('.//text()').extract()
            raw_row = may_strip(''.join(raw_row))
            # process only rows between `SUB` header and `=====` horizontal line
            if raw_row.startswith('SUB'):
                do_process = True
            elif raw_row.startswith('===='):
                do_process = False

            if do_process:
                # discard empty rows or section delimiters
                if not may_strip(raw_row):
                    continue
                if may_strip(raw_row) in ('-', 'SUB', 'SUBS', 'FLD', 'FXD'):
                    continue

                # cleanup row so that data can be extracted easier later
                row = self.clean_row(raw_row)
                if not row:
                    self.logger.info('Unable to parse row:\n%s', may_strip(raw_row))
                    continue

                raw_item = {str(cell_idx): may_strip(cell) for cell_idx, cell in enumerate(row)}
                raw_item.update(
                    provider_name=self.provider, reported_date=may_strip(mail.envelope['subject'])
                )
                yield normalize.process_item(raw_item)

    @staticmethod
    def clean_row(raw_row):
        """Clean raw row and add proper delimiters between different columns.

        Example of a raw row:
            - UACC AL MEDINA 35 CPP BAHRAIN/EAFR-AG 02-03 SEP W117.5-140K MERCURIA FXD
            - GEM NO.3 55 NAP JUBAIL/JAPAN 06 SEP W100 ATC SUB
            - CHAMPION PLEASURE FIXED 75KT NAP KWT/JAPAN 05 MAY O/P CHEVRON

        Args:
            row (str):

        Returns:
            Tuple[str] | None:

        """
        # each line represents one column to be matched
        # new pattern
        pattern = (
            r'([\w\s\-\.0-9]+)\s+'  # vessel name
            r'(FIXED|ON SUBS|FAILED)\s+'  # charter status
            r'(?:(\d+)(KT|KY)\s+)?'  # quantity of product chartered, if any (KY is a typo of KT)
            r'([A-Z\+]+)\s+'  # product chartered, if any
            r'([A-Z\s\.]+)\/'  # departure zone
            r'([A-Z\s\-\+\.]+)\s+'  # arrival zone
            r'([\d\-]*\s[A-Z]{3})\.?\s+'  # laycan period
            r'(\S{3,})\s+'  # rate value
            r'([\w\'\s]{2,})'  # charterer
        )

        row = may_strip(raw_row)

        if re.search(pattern, row):
            return re.search(pattern, row).groups()

        MISSING_ROWS.append(row)

        return None

    @property
    def missing_rows(self):
        return MISSING_ROWS
