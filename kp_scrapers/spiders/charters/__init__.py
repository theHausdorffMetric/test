from kp_scrapers.spiders.bases.markers import CoalMarker, CppMarker, LngMarker, LpgMarker, OilMarker


class CharterSpider(CoalMarker, LngMarker, LpgMarker, OilMarker, CppMarker):
    category_settings = {'DATADOG_CUSTOM_TAGS': ['category:charter']}

    @classmethod
    def category(cls):
        return 'charter'
