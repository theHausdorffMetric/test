from csv import DictReader
from io import StringIO
import re

from dateutil.parser import parse as parse_date
from scrapy import Spider
from scrapy.http import FormRequest

from kp_scrapers.lib.date import to_isoformat
from kp_scrapers.lib.utils import to_unicode
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.models.spot_charter import SpotCharter
from kp_scrapers.models.utils import validate_item
from kp_scrapers.spiders.charters import CharterSpider


USER_AGENT = "Apache-HttpClient/UNAVAILABLE (java 1.4)"
DOMAIN = 'tiinfo.net'
LOGIN_PAGE = 'https://{}/ValidateUserRights.aspx'.format(DOMAIN)
FIXTURE_PAGE = 'https://{}/PullFixtureStreaming.aspx'.format(DOMAIN)
SUPPORTED_API_VERSION = '1.0'
FIELDNAMES = [
    'fixture_id',  # seems to be an integer that is incremented
    'charter_contract_status',
    'lay_can_start',
    'lay_can_end',
    'vessel_name',
    'voyage_origin_destination',
    'charterer',
    'rate_value',
    'pricing_type',  # 1 = WS rate or no rate; 2 = total in USD
    'reported_date',
    'vessel_operator',
    # Seems to be an update date for this fixture : always > to report_date
    '?NOT_DISPLAYED_date_update?',
    # 0 if not failed, date of failure otherwise
    'failure_date',
    # Seems to be an date of update for this fixture : always > to report_dat
    # and to ?NOT_DISPLAYED_date_update?
    '?NOT_DISPLAYED_date_update_2?',
    'rate_year',
    'idle_days',
    'rv_TCE_per_day_excluding_idle_days',
    'actual_TCE_per_day_including_idle_days',
    'total_days',
    'size_metric_tons',
    'laytime_demurrage_in_hours',
    'laytime_demurrage_cost_per_day_in_usd',
    'broker_address_commission_min_percent',
    'broker_address_commission_max_percent',
    'last_done_rate_value',
    'last_done_pricing_type',
    'last_done_year',
    'last_done_report_date',
    'last_done_size_metric_tons',
    '?variation?',
    'coming_from',
    'open_date',
    'total_days_excluding_idle',
    '?NOT_DISPLAYED_next_destination?',
    'speed_ballast',
    'speed_laden',
    'vessel_age',
    'voyage_origin_destination2',
    # Maybe WS values or WS rates ?
    '?NOT_DISPLAYED_rate_1?',
    '?NOT_DISPLAYED_rate_2?',
    '?NOT_DISPLAYED_rate_3?',
    '?NOT_DISPLAYED_rate_4?',
    '?NOT_DISPLAYED_rate_5?',  # noqa
    # Estimated flags
    'estimated_flag_rate',  # when 1 indicate that rate  is not sure
    'estimated_flag_voyage_origin_destination',  # when 1 indicate that voyage is not sure
    'estimated_flag_charterer',  # when 1 indicate that charterer is not sure
    'estimated_flag_vessel',  # when 1 indicate that vessel identity is not sure
    'estimated_flag_lay_can',  # when 1 indicate that date of lay_can are not sure
    # Maybe WS values or WS rates ?
    '?NOT_DISPLAYED_rate_11?',
    '?NOT_DISPLAYED_rate_12?',
    'breakeven_TCE_in_usd_per_day',
    '10_percent_return_TCE_in_usd_per_day',
    # if deviation_from_last_done_absolute_ratio > 0.9 then 1 else 0
    # Trigger double arrow (down and red or up and green) and asterisk in VLCC app
    'high_deviation_from_last_done_flag',
    'deviation_from_last_done_absolute_ratio',  # x100 to have the absolute % of deviation
    # 0 or 1, seems to be 1 when estimated_flag_charterer is 1, but not for all
    '?UNKNOWN_flag?',
]


@validate_item(SpotCharter, normalize=True, strict=False)
def _spot_charter_factory(row, **kwargs):
    item = {
        'lay_can_start': to_isoformat(row['lay_can_start'], dayfirst=False),
        'lay_can_end': to_isoformat(row['lay_can_end'], dayfirst=False),
        'reported_date': parse_date(row['reported_date'], dayfirst=False).strftime('%d %b %Y'),
        # 'open_date': row['open_date'],
        # 'coming_from': row['coming_from'],
        # 'fixture_id': row['fixture_id'],
        'charterer': row['charterer'],
        'seller': row['vessel_operator'],
        'status': row['charter_contract_status'],
        # 'broker_address_commission_max': row['broker_address_commission_max_percent'],
        # 'last_done_rate_value': row['last_done_rate_value'],
        'rate_value': row['rate_value'] if row['rate_value'] else None,
        # 'actual_tce_per_day_including_idle_days': row['actual_TCE_per_day_including_idle_days'],
        # 'breakeven_tce': row['breakeven_TCE_in_usd_per_day'],
        # 'voyage_raw_text': row['voyage_origin_destination'],
        # 'voyage_raw_text2': row['voyage_origin_destination2'],
        **kwargs,
    }

    match_vessel = re.match('([\w\s.]+) \(([0-9]{4})\)', row['vessel_name'])
    match_origin_dest = re.match('([\'\w]+)/([\'\w]+)', row['voyage_origin_destination'])

    if match_vessel:
        item['vessel'] = {'name': match_vessel.group(1), 'build_year': match_vessel.group(2)}
    else:
        item['vessel'] = {'name': row['vessel_name']}

    if match_origin_dest:
        item['departure_zone'] = match_origin_dest.group(1)
        item['arrival_zone'] = [match_origin_dest.group(2)]
        match_origin = re.match('(\w+)via\w+', match_origin_dest.group(1))
        match_dest = re.match('(\w+)via\w+', match_origin_dest.group(2))

        if match_origin:
            item['departure_zone'] = match_origin.group(1)

        if match_dest:
            item['arrival_zone'] = [match_dest.group(1)]

    return item


class VLCCFixturesSpider(CharterSpider, Spider):
    name = 'VLCCFixtures'
    provider = 'VLCCF'
    version = '1.0.1'
    produces = [DataTypes.SpotCharter, DataTypes.Vessel]

    allowed_domains = [DOMAIN]

    def start_requests(self):
        headers = {'User-agent': USER_AGENT, 'Host': DOMAIN, 'Content-Length': 0}
        return [
            FormRequest(
                LOGIN_PAGE,
                headers=headers,
                formdata={},
                # needed to be forced because FormRequest with
                # empty formdata sent a GET
                method='POST',
                callback=self.logged_in,
            )
        ]

    def logged_in(self, response):
        headers = {
            'User-agent': USER_AGENT,
            'Host': DOMAIN,
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': 0,
        }
        yield FormRequest(
            FIXTURE_PAGE,
            headers=headers,
            formdata={},
            # formdata={'after': '201504120944', 'before': '201604120944', },
            method='POST',
            callback=self.parse_results,
        )

    def parse_results(self, response):
        sep = ';'
        rows = []
        groups = response.body.split(b';')

        # Not sure, seems that first group is the API version, seems to be 1.0 all the time
        version_api = to_unicode(groups[0])
        # Second group is the number of field per row  ;
        n_col = int(groups[1])

        # Fixture data start after the 2 first group
        data = groups[2:]

        # Modify spider and update the '1.0' if the API version change.
        if version_api != SUPPORTED_API_VERSION:
            self.logger.error('VLCC fixture new version API. Change to spider probably required')
        # Check that the data are consistent : can we create complete rows
        elif len(data) % n_col != 0:
            self.logger.error('Probably a new column has been added. Change to spider required')
        else:
            while len(data):
                data = [to_unicode(i) for i in data]
                rows.append(sep.join(data[:n_col]))
                data = data[n_col:]
            reader = DictReader(StringIO('\n'.join(rows)), fieldnames=FIELDNAMES, delimiter=sep)
            for row in reader:
                yield _spot_charter_factory(row, provider_name=self.provider)
