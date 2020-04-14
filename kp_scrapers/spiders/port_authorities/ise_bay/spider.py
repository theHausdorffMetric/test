from typing import Any, Dict, Iterator, Optional
import unicodedata

from scrapy import Spider
from scrapy.http import Response

from kp_scrapers.lib.parser import may_strip, row_to_dict
from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.ise_bay import normalize


def _standardize_char_width(string: Optional[str]) -> Optional[str]:
    """Standardize half-width/full-width display of characters.

    Source is inconsistent in displaying characters as full-width and half-width.

    This function applies the following unicode normalisation:
        - katakana: half-width -> full-width
        - ASCII:    full-width -> half-width

    Note that 'NFKC' stands for
    "Normalization Form KC [Compatibility Decomposition, followed by Canonical Composition]"

    Examples:
        >>> _standardize_char_width('ｵｰｽﾄﾗﾘｱ')
        'オーストラリア'
        >>> _standardize_char_width('ガスタンカー')
        'ガスタンカー'
        >>> _standardize_char_width('ＪＦＥ ﾏｰｷｭﾘｰ １２３')
        'JFE マーキュリー 123'
        >>> _standardize_char_width('foobar 123')
        'foobar 123'

    """
    return unicodedata.normalize('NFKC', string) if string else None


class IseBaySpider(PortAuthoritySpider, Spider):
    name = 'IseBay'
    provider = 'IseBay'
    version = '2.0.0'
    produces = [DataTypes.PortCall]

    start_urls = ['http://www6.kaiho.mlit.go.jp/isewan/schedule/IRAGO/schedule_3.html']

    def parse(self, response: Response) -> Iterator[Optional[Dict[str, Any]]]:
        """Parse reponse from IseWan Vessel Traffic Service Centre website."""

        reported_date = response.xpath('//div[@class="_inner"]/p/text()').extract_first()
        events = []  # to hold sequential list of vessel lineup for advanced parsing

        table = response.xpath('//table[@class="generalTB"]')
        for row_idx, row in enumerate(table.xpath('.//tr')):
            # first row of source table is always the header
            if row_idx == 0:
                headers = row.xpath('.//th/text()').extract()
                continue

            # subsequent rows are exclusively vessel movements only
            raw_item = row_to_dict(row, headers)

            # contextualise item with meta info
            raw_item.update(provider_name=self.provider, reported_date=reported_date)

            # standardize character width
            for key, value in raw_item.items():
                raw_item[key] = may_strip(_standardize_char_width(value))

            event = normalize.process_item(raw_item)
            events.append(event) if event else None

        # combine arrival and departure events into a single 'PortCall' datatype
        for event in events:
            yield from normalize.combine_event(event, events)
