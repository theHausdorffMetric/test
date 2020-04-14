from kp_scrapers.spiders.bases.persist import PersistSpider
from kp_scrapers.spiders.operators import OperatorSpider
from kp_scrapers.spiders.operators.extractors.gate import ExcelExtractorGate


class GateOperatorSpider(OperatorSpider, PersistSpider):
    name = 'GateOperator'
    provider = 'Gate'
    version = '0.1.0'
    produces = []  # TODO formalise OperatorSpider data model

    start_urls = [
        'https://www.gateterminal.com/wp-content/themes/minimal210-child/Huidig-gebruik-bestand/gate_stats.xls'  # noqa
    ]

    def parse(self, response):
        xl_obj = ExcelExtractorGate(response.body, response.url, self.start_date)
        return xl_obj.parse_excel()
