import datetime as dt

from scrapy import Spider

from kp_scrapers.models.normalize import DataTypes
from kp_scrapers.spiders.port_authorities import PortAuthoritySpider
from kp_scrapers.spiders.port_authorities.elsalvador import normalize


# table headers are split across 2 rows in the source, easier to define here
TABLE_HEADER = [
    'vessel_name',  # Llegada - Buque
    'vessel_draft',  # Calados (Mtrs) PROA/POPA
    'vessel_length',  # Eslora (Mtrs)
    'vessel_gt',  # T.R.B
    'shipping_agent',  # Agencia Naviera
    'eta_day',  # Arribo Estimado Día
    'eta_hours',  # Arribo Estimado Hora
    'useless_field',  # does not map to anything
    'is_discharge',  # Operación Des.
    'is_load',  # Operación Est.
    'tons_moved',  # A Movilizar Ton.
    'units_moved',  # A Movilizar Unid.
    'operation_time',  # Tiempo Operación Horas
    'expected',  # Anticipo
    'client',  # Planta Receptora o Expeditora
    'cargo',  # CARGA
]


class ElSalvadorSpider(PortAuthoritySpider, Spider):
    name = 'ElSalvador'
    provider = 'El Salvador'
    version = '1.1.1'
    produces = [DataTypes.PortCall, DataTypes.Vessel, DataTypes.Cargo]

    start_urls = [
        'http://sadfiweb.gob.sv:8090/ConsultasEnLinea/BuquesAnunciados/BuquesAnunciados.php'
    ]

    port_name = 'Acajutla'

    def parse(self, response):
        # reported date from source is inaccurate, define from time scrapped
        reported_date = (
            dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        )

        # First 2 lines are headers, skip
        for row in response.xpath('//*[@class="datatable"]//tr')[2:]:
            # build raw item manually instead of using `row_to_dict` as there could be multiple
            # cargoes which would've been concatenated together
            # skip idx 7, not a table column
            raw_item = {
                TABLE_HEADER[idx]: cell.xpath('.//text()').extract()
                for idx, cell in enumerate(row.xpath('td'))
                if idx != 7
            }
            # contextualise raw item with metadata
            raw_item.update(
                port_name=self.port_name, provider_name=self.provider, reported_date=reported_date
            )
            yield normalize.process_item(raw_item)
