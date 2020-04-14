"""Rotterdam spider module

Website: https://www.portofrotterdam.com/en/shipping/operational-information/nautical-information/arrivals-and-departures-of-vessels  # noqa

Source provides data on expected vessels, vessels currently in port, and recently departed vessels.
To obtain all the data, an unofficial "API" used by the website itself to retrieve all portcalls,
is called.

"""
import datetime as dt
import json

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.rotterdam import normalize


class RotterdamSpider(PortAuthoritySpider, Spider):
    name = 'Rotterdam'
    provider = 'Rotterdam'
    version = '1.1.0'
    produces = [DataTypes.PortCall, DataTypes.Vessel]

    start_urls = ['https://www.portofrotterdam.com/ship_visits/all']

    def __init__(self, event=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # supported types : Departed, Present, Expected
        event = event if event else 'Expected'
        self.events = [type_.strip() for type_ in event.split(',')]

    def parse(self, response):
        # memoise reported date so it won't need to be called repeatedly later
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0).isoformat(timespec='seconds')
        )

        json_obj = json.loads(response.text)
        for event in self.events:
            # sanity check; in case event type does not exist anymore
            if not json_obj.get(event):
                self.logger.error("Event type does not exist in response: %s", event)
                continue

            for raw_item in json_obj[event]:
                # contexualise raw item with meta info
                raw_item.update(
                    event_type=event,
                    port_name=self.name,
                    provider_name=self.name,
                    reported_date=reported_date,
                )
                yield normalize.process_item(raw_item)
