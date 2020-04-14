"""Spider module for GibsonRegistry spider.

TODO docs on design considerations

Usage
~~~~~

    $ scrapy crawl GibsonRegistry \
        -a reported_date=20190314

"""
import csv
import datetime as dt
import re

from kp_scrapers.constants import BLANK_START_URL
from kp_scrapers.lib.compression import zip_uncompress
from kp_scrapers.lib.services import s3
from kp_scrapers.lib.static_data import fetch_kpler_fleet
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.registries import RegistrySpider
from kp_scrapers.spiders.registries.gibson import api, normalize


SOURCE_BUCKET = 'kp-datalake-shared'
SOURCE_KEY_PREFIX = 'trigonal/EAGKpler'
SOURCE_KEY_PATTERN = r'EAGKpler(?P<reported_date>\d{8})\d{4}\.zip'
TABLES_TO_KEEP = r'EAGKpler\w+\.txt'


class GibsonRegistryBaseSpider(RegistrySpider, PersistSpider):
    """Base module for getting vessels data from uploaded S3 file with generic queries.

    By default, GibsonRegistry spider will extract vessels from a reconstructed DB snapshot
    according to the query stored in `./query/retrieve-records.sql`.

    """

    name = 'GibsonRegistry'
    # NOTE provider name is deliberately obfuscated due to the sensitive nature of the source
    provider = 'GR'
    version = '1.7.0'
    produces = [DataTypes.Vessel]

    start_urls = [BLANK_START_URL]

    def __init__(self, reported_date=None, query=None, force_load=False, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # get persistence state
        self.files_extracted = self.persisted_data.get('files_extracted', [])

        # `reported_date` is used to filter and get latest files uploaded to S3 store
        # `reported_date` string must be formatted as YYYYMMDD
        # use current date as `reported_date` if not provided as argument
        self.reported_date = (
            reported_date if reported_date else dt.datetime.utcnow().strftime('%Y%m%d')
        )
        self.iso_reported_date = dt.datetime.strptime(self.reported_date, '%Y%m%d').isoformat()

        # custom data query; this will override the default query specified
        self.query = query
        self.force_load = force_load

    def parse(self, _):
        """Entrypoint for GibsonRegistry spider.

        Args:
            _ (scrapy.Response): redundant response

        Yields:
            Dict[str, str]:

        """
        # DB (SQLite) is used as there are too many sub-files to parse and join,
        # and they were obtained as CSV extracts from the provider's DB anyway.
        # Use in-memory DB for performance.
        db = api.VesselsDB(path='file::memory:?cache=shared')

        # populate sqlite tables with S3 data
        for s3obj in s3.iter_files(SOURCE_BUCKET, Prefix=SOURCE_KEY_PREFIX):
            # filter files having specified `reported_date`
            file_match = re.search(SOURCE_KEY_PATTERN, s3obj.key)
            if not file_match or file_match.group('reported_date') != self.reported_date:
                continue

            # check if files on specified date has been extracted already
            if not self._check_persistence() and not self.force_load:
                return

            # download zipped folder
            s3_file = next(s3.fetch_file(SOURCE_BUCKET, key_name=s3obj.key))

            # uncompress zipped folder
            for file_name, rows in zip_uncompress(s3_file, TABLES_TO_KEEP, reader=csv.reader):
                # sanity check in case csv extract does not contain any rows
                if not rows:
                    self.logger.error(f'No data found in extract: {s3_file.key}/{file_name}')
                    return

                # table name is identical to file name sans extension
                db.set_rows(table_name=file_name.split('.txt')[0], rows=rows)

        # retrieve vessels according to specified sql query
        for raw_item in db.get_rows(self.query):
            # contextualise raw item with meta info
            raw_item.update(
                provider_name=self.provider, reported_date=self.iso_reported_date,
            )
            yield normalize.process_item(raw_item)

    def _check_persistence(self):
        """Check if the vessel data to be processed has been scraped previously.

        This function allows us to run this spider hourly and not scrape the same data repeatedly.
        This is done because the provider will only update the data once every week.

        Returns:
            bool: True if data has not been scraped previously, else False

        """
        if self.reported_date in self.files_extracted:
            self.logger.info(f'Data already extracted previously: {self.reported_date}')
            return False

        # save persistence state
        self.files_extracted.append(self.reported_date)
        self.persisted_data.update(files_extracted=self.files_extracted)
        self.persisted_data.save()

        return True


class GibsonRegistryActiveSpider(GibsonRegistryBaseSpider):
    """Spider module for getting Kpler vessels data from S3 uploaded files.

    This spider will yield vessels data for vessels already existing in our DB,
    and nothing else.

    """

    name = 'GibsonRegistryActive'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cross reference Kpler's fleet with Gibson's, and update our fleet
        self._fleet = tuple(v['imo'] for v in fetch_kpler_fleet(lambda _: True, disable_cache=True))
        self.query = ' '.join(
            (
                api.VesselsDB._sql_script('retrieve-records.sql'),
                f'WHERE EAGKplerVesselMain.IMONumber IN {repr(self._fleet)};',
            )
        )


class GibsonRegistryNewbuildSpider(GibsonRegistryBaseSpider):
    """Spider module for getting unseen vessels cross-referenced with Kpler vessels.

    This spider will yield unknown vessels that we have yet to see on our platforms.
    Whether it will be loaded will still be subject to loader constraints.

    """

    name = 'GibsonRegistryNewbuild'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cross reference Kpler's fleet with Gibson's
        self._fleet = tuple(v['imo'] for v in fetch_kpler_fleet(lambda _: True, disable_cache=True))
        self.query = ' '.join(
            (
                api.VesselsDB._sql_script('retrieve-records.sql'),
                # prevent yielding of vessels we already have
                f'WHERE EAGKplerVesselMain.IMONumber NOT IN {repr(self._fleet)}',
                # take only active and under construction vessels
                'AND EAGKplerTradingStatus.tradingcategorycode IN ("New", "Live")',
                'AND EAGKplerVesselMain.vesseltypecode IN ("LNG", "LPG", "Offs", "Tank", "Chem", "Comb", "Bulk", "Gen", "Misc")',  # noqa
                # sanity check
                'AND EAGKplerVesselMain.DWT != ""',
                'AND EAGKplerVesselMain.GT != ""',
                'AND EAGKplerVesselMain.LOA != ""',
                'AND EAGKplerVesselMain.BeamMoulded != ""',
                'AND EAGKplerVesselMain.flagcode != ""',
                'AND EAGKplerVesselMain.builtyear != ""',
                # take only vessels from last year onwards
                f'AND cast(EAGKplerVesselMain.builtyear AS int) >= {dt.datetime.utcnow().year - 1}',
            )
        )
