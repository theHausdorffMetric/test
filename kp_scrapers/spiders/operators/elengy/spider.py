import datetime as dt
from typing import Any, Dict, Iterator, Optional, Union

from scrapy import Spider
from scrapy.http import FormRequest, Response

from kp_scrapers.lib.parser import row_to_dict
from kp_scrapers.spiders.operators.elengy import normalize


class ElengyOperatorMixin:
    __TERMINAL_IDS = {'Fos Tonkin': '2', 'Montoir': '1'}
    __QUERY_TYPE_IDS = {'flow': '1', 'capacity': '2'}

    start_urls = [
        'https://www.elengy.com/en/contracts-and-operations/operational-management/use-data/recherches.html?article1=92&article2=106',  # noqa
    ]

    def __init__(
        self, terminal: str, query_type: str, lookbehind_days: Optional[str] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)

        if terminal not in self.__TERMINAL_IDS.keys():
            raise ValueError(f"Unknown Elengy terminal: '{terminal}'")

        if query_type not in self.__QUERY_TYPE_IDS.keys():
            raise ValueError(f"Unknown Elengy query type: '{query_type}'")

        self.terminal = self.__TERMINAL_IDS[terminal]
        self.query_type = self.__QUERY_TYPE_IDS[query_type]

        # lower bound of date range to send to API for searching inventory levels
        if lookbehind_days:
            self.start_date = dt.date.today() - dt.timedelta(days=int(lookbehind_days))
        else:
            # source only has data available from 2011-03-03 onwards
            self.start_date = dt.date(year=2011, month=3, day=3)

        # upper bound of date range to send to API for searching inventory levels
        # source typically has data only up to end of current month
        # but sometimes data will be available for the next 2 months
        self.end_date = dt.datetime.utcnow() + dt.timedelta(days=90)

        # memoise reported_date so it won't need to be called repeatedly later on
        self.reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

    def parse(self, response: Response) -> Iterator[FormRequest]:
        token = self._get_request_token(response)
        # no point to continue if we cannot get a token to authenticate our requests
        if not token:
            self.logger.error("Unable to retrieve request token; resource may have changed")
            return

        formdata = {
            'jform[terminal]': self.terminal,
            'jform[type]': self.query_type,
            'jform[jour1]': str(self.start_date.day),
            'jform[mois1]': str(self.start_date.month),
            'jform[annee1]': str(self.start_date.year),
            'jform[jour2]': str(self.end_date.day),
            'jform[mois2]': str(self.end_date.month),
            'jform[annee2]': str(self.end_date.year),
            'jform[start]': '1',
            'jform[export]': '0',
            'option': 'com_transparence',
            'view': 'recherches',
            'submit': 'Visualiser',
            token: '1',
        }

        yield FormRequest(
            url=self.start_urls[0],
            callback=self.paginate,
            formdata=formdata,
            cb_kwargs={'formdata': formdata},
        )

    def paginate(
        self, response: Response, formdata: Dict[str, str]
    ) -> Iterator[Union[FormRequest, Dict[str, Any]]]:
        """Handle response pagination.

        Source will paginate results like so:

            " << Première < Précédente [CURRENT_PAGE_NUMBER] Suivante > Dernière >> "

        """
        yield from self.extract_current_page(response)

        last_page = response.xpath(
            '//div[@class="pagination-bloc"]/a[contains(text(), "Dernière")]/@data-page'
        ).extract_first()
        if not last_page:
            self.logger.debug("No pagination for current response")
            return

        if not last_page.isnumeric():
            self.logger.error("Unable to retrieve pagination; resource may have changed")
            return

        # last page as given by the HTML response is actually the penultimate page,
        # so we need to increment last page number by two
        for page in range(2, int(last_page) + 2):
            formdata['jform[start]'] = str(page)
            yield FormRequest(
                url=self.start_urls[0], callback=self.extract_current_page, formdata=formdata,
            )

    def extract_current_page(self, response: Response) -> Iterator[Dict[str, Any]]:
        """Extract data from current HTML page.

        Each page contains a table of the flow/capacity levels of the specified terminal.

            | Jour       | Stock GNL à 6h | Quantités nominées | Quantités allouées |
            |------------|----------------|--------------------|--------------------|
            | 10/03/2020 |       22       |     115 091 562    |     115 091 562    |
            | 11/03/2020 |       77       |     115 091 562    |     115 091 562    |
            | 12/03/2020 |       59       |     115 091 562    |     115 091 562    |
            | 13/03/2020 |       42       |     115 091 562    |     115 091 562    |
            | 14/03/2020 |       25       |     115 091 562    |     115 091 562    |
            | 15/03/2020 |       80       |     73 987 433     |     73 987 433     |
            | 16/03/2020 |       69       |     24 662 478     |         -          |
            | 17/03/2020 |       65       |     24 662 478     |         -          |
            | 18/03/2020 |       61       |     24 662 478     |         -          |
            | 19/03/2020 |       57       |     24 662 478     |         -          |
            | 20/03/2020 |       53       |     24 662 478     |         -          |
            |    ...     |       ...      |        ...         |        ...         |


        """
        # date_format = "%d/%m/%Y"
        headers = response.xpath('//table[@summary=""]/thead//th/text()').extract()
        if len(headers) != 4:
            self.logger.error("Unable to extract data; resource may have changed")
            return

        rows = response.xpath('//table[@summary=""]/tbody//tr')
        for row in rows:
            raw_item = row_to_dict(row, headers)
            # append meta info
            raw_item.update(
                # TODO should be changed to a more descriptive provider name, like 'Elengy'
                provider_name=self.provider,
                reported_date=self.reported_date,
            )

            yield normalize.process_item(raw_item)

    def _get_request_token(self, response: Response) -> Optional[str]:
        """Get token required for form request in this spider to succeed."""
        html_res = response.xpath('//form/input[@type="hidden" and @value="1"]')
        return html_res.xpath('./@name').extract_first() if len(html_res) == 1 else None


class FosTonkinOperator(ElengyOperatorMixin, Spider):
    name = 'FosTonkinOperator'
    version = '1.1.0'
    provider = 'FosTonkinOperator'
    produces = []  # TODO fill in appropriate datatype


class MontoirOperator(ElengyOperatorMixin, Spider):
    name = 'MontoirOperator'
    version = '1.1.0'
    provider = 'MontoirOperator'
    produces = []  # TODO fill in appropriate datatype
