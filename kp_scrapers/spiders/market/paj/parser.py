from datetime import datetime as dt
import re

from kp_scrapers.lib.parser import may_strip


def parse_date_range(date_range_raw):
    """Parse the incoming date
        Examples:
            >>> parse_date_range('18/Aug/2020 - 24/Aug')
            ('2020-08-18T00:00:00', '2020-08-24T00:00:00')
            >>> parse_date_range('Current Week（18/Aug/2020 - 24/Aug）')
            ('2020-08-18T00:00:00', '2020-08-24T00:00:00')
            >>> parse_date_range('18/Aug/2019 - 24/sep/2019')
            ('2019-08-18T00:00:00', '2019-09-24T00:00:00')
            >>> parse_date_range('18/Aug/2019 - 24-sep/2019')
            (None, None)

        """
    matched_items = re.search(
        r"(?P<start>[0-9]{1,2}/[A-Za-z]{3}/[0-9]{4})"
        "\s*-\s*"
        "(?P<end>[0-9]{1,2}/[A-Za-z]{3}/*[0-9]{0,4})",
        date_range_raw,
    )

    if not matched_items:
        return None, None

    start_range = dt.strptime(matched_items.group('start'), '%d/%b/%Y')
    end_range = matched_items.group('end')

    if len(end_range.split('/')) == 2:
        end_range = end_range + '/' + str(dt.now().year)

    end_range = dt.strptime(end_range, '%d/%b/%Y')

    return start_range.isoformat(), end_range.isoformat()


def get_country(response):
    """ get country from the webpage
    """
    country = response.xpath('//h3//text()').extract_first().replace('-', '').strip()
    if 'ALL' in country:
        country_type = 'country'
    else:
        country_type = 'custom'

    return country, country_type


def get_reported_date(response):
    """ get reported date from webpage
    """

    # the reported date is not under any tag so we have to use the magic numbers div[3] and
    # again refer [2] to get the reported date.
    report_date_raw = response.xpath('//div[3]//text()').extract()
    cleaned_reported_date = may_strip(report_date_raw[2]).replace('[', '').replace(']', '')
    return dt.strptime(cleaned_reported_date, '%d/%b/%Y')
