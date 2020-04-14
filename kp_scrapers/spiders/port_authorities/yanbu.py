# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta

from scrapy import Spider
from scrapy.http import FormRequest
from scrapy.selector import Selector

from kp_scrapers.models.items import (
    ArrivedEvent,
    DepartedEvent,
    EtaEvent,
    EtdEvent,
    VesselIdentification,
)
from kp_scrapers.spiders.bases.markers import LpgMarker
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider


class YanbuSpider(LpgMarker, PortAuthoritySpider, Spider):
    """In the formdata, we yield a request that takes into account several inputs :

    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$SearchType" :
        2 possible values : "rbVesselSearch" or "rbAgentSearch".
        We set it as "rbVesselSearch" to scrape the vessels scheduled.

    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$ddlPort" :
        ranges from 1 to 9, each value corresponds to a port :
        "1": Dammam Port
        "2": Dhiba Port
        "3": "Jeddah Port"
        "4": "Jizan Port"
        "5": "Jubail Industrial"
        "6": "Jubail Port"
        "7": "Yanbu Industrial"
        "8": "Yanbu Port"
        "9": "Ras Alkhair Port"

    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$tgAgent" :
        filter by agent name.

    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$tbVessel" :
        filter by vessel name.
    http://www.ports.gov.sa/English/Pages/admin.aspx
    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$dtcStartDate$dtcStartDateDate" :
        start date of the data to scrape.
        format : "dd/yy/dddd"

    "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$ctl00$dtcEndDate$dtcEndDateDate" :
        End date of the data to scrape.
        format : "dd/mm/yyyy"

    """

    name = 'Yanbu'
    version = '1.0.0'
    # NOTE cf comment of Jean in Airtable:
    # > The source is not "Yanbu Port Authority" but Saudi Port Authority that
    # > cover 9 ports, including Yanbu Industrial
    provider = 'Yanbu'

    start_urls = ['https://www.ports.gov.sa/English/Pages/admin.aspx']

    search_type = 'rbVesselSearch'
    ports_to_scrap = {
        '3': 'Jeddah Port',
        '5': 'Jubail Industrial',
        '7': 'Yanbu Industrial',
        '9': 'Ras Alkhair Port',
    }
    agent_filter = ''
    vessel_filter = ''
    start_date = datetime.strftime(datetime.now() - timedelta(weeks=1), '%d/%m/%Y')
    end_date = datetime.strftime(datetime.now() + timedelta(days=30), '%d/%m/%Y')

    def parse(self, response):
        selector = Selector(response)
        last_focus = selector.xpath('//input[@id="__LASTFOCUS"]/@value').extract()
        view_state = selector.xpath('//input[@id="__VIEWSTATE"]/@value').extract()
        event_validation = selector.xpath('//input[@id="__EVENTVALIDATION"]/@value').extract()

        for i in self.ports_to_scrap.keys():

            formdata_dict = {
                "MSOWebPartPage_PostbackSource": "",
                "MSOTlPn_SelectedWpld": "",
                "MSOTlPn_View": "0",
                "MSOTlPn_ShowSettings": "False",
                "MSOGallery_SelectedLibrary": "",
                "MSOGallery_FilterString": "",
                "MSOTlPn_Button": "none",
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__REQUESTDIGEST": "0x998ADB472F06BAFEDEE17"
                "03161F295CB77AC40A1F38A"
                "4B8FE42D09DA5291200F452"
                "D9BC2020DCA57FE7BA94F6B"
                "BAFFED8610325F279A40BC5"
                "110AEE78C36D1B6,31+Aug+"
                "2016+14:01:12+-0000",  # noqa
                "MSOSPWebPartManager_DisplayModeName": "Browse",
                "MSOPWebPartManager_ExitingDesignMode": "false",
                "MSOWebPartPage_Shared": "",
                "MSOLayout_LayoutChanges": "",
                "MSOLayout_InDesignMode": "",
                "_wpSelected": "",
                "_wzSelected": "",
                "MSOSPWebPartManager_OldDisplayModeName": "Browse",
                "MSOSPWebPartManager_StartWebPartEditingName": "false",
                "MSOSPWebPartManager_EndWebPartEditing": "false",
                "__LASTFOCUS": last_focus,
                "__VIEWSTATE": view_state,
                "__EVENTVALIDATION": event_validation,
                "InputKeywords": "Search+this+site...",
                "ctl00$PlaceHolderSearchArea$ctl01$" "ctl03": "0",
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$"
                "ctl00$SearchType": "rbVesselSearch",
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$" "ctl00$ddlPort": i,
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$"
                "ctl00$tgAgent": self.agent_filter,
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$"
                "ctl00$tbVessel": self.vessel_filter,
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$" "ctl00$ddlStatus": "-1",
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$"
                "ctl00$dtcStartDate$dtcStartDateDate": self.start_date,
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$"
                "ctl00$dtcEndDate$dtcEndDateDate": self.end_date,
                "ctl00$m$g_15009805_4f7a_41c3_a2b2_b7aefb00b1ec$" "ctl00$btnSearch": "Search",
                "__spText1": "",
                "__spText2": "",
                "_wpcmWpid": "",
                "wpcmVal": "",
            }

            yield FormRequest(
                url="https://www.ports.gov.sa/English/Pages/admin.aspx",
                method="POST",
                formdata=formdata_dict,
                callback=self.parse_table,
                meta={'port_name': self.ports_to_scrap[i]},
            )

    def parse_table(self, response):
        selector = Selector(response)
        xpath_ = (
            '//table[@id="ctl00_m_g_15009805_4f7a_41c3_a2b2_' 'b7aefb00b1ec_ctl00_gvSearchResult"]'
        )  # noqa
        table = selector.xpath(xpath_)
        meta = response.meta
        port_name = meta['port_name']

        for i, row in enumerate(table.css('tr')):
            if i == 0:
                continue
            status = self._field(row, 1)
            arrival_date = self._field(row, 2)
            departure_date = self._field(row, 3)
            agent = self._field(row, 4)
            vessel_type = self._field(row, 5)
            # TODO: validate that this fiedl is Gross tonnage
            # There are MTS and MBS units ??
            # tonnage = self._field(row, 6)

            vessel_name = self._field(row, 7)

            vessel_base = VesselIdentification(name=vessel_name, type=vessel_type)

            if status == 'Arrival':
                yield ArrivedEvent(
                    vessel=vessel_base,
                    arrival=arrival_date,
                    port_name=port_name,
                    shipping_agent=agent,
                )
                yield EtdEvent(
                    vessel=vessel_base,
                    etd=departure_date,
                    port_name=port_name,
                    shipping_agent=agent,
                )

            elif status == 'Departed':
                yield ArrivedEvent(
                    vessel=vessel_base,
                    arrival=arrival_date,
                    port_name=port_name,
                    shipping_agent=agent,
                )
                yield DepartedEvent(
                    vessel=vessel_base,
                    departure=departure_date,
                    port_name=port_name,
                    shipping_agent=agent,
                )

            elif status == 'Expected':
                yield EtaEvent(
                    vessel=vessel_base, eta=arrival_date, port_name=port_name, shipping_agent=agent
                )
                yield EtdEvent(
                    vessel=vessel_base,
                    etd=departure_date,
                    port_name=port_name,
                    shipping_agent=agent,
                )

    @staticmethod
    def _field(line, i):
        q = 'td:nth-child({}) *::text'.format(i)
        return ''.join(line.css(q).extract()).strip()
