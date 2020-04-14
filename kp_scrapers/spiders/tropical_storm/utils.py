from functools import wraps
from io import BytesIO
from zipfile import ZipFile

from scrapy.selector import Selector


def unpack_kml(func):
    @wraps(func)
    def _inner(klass, response):
        z = ZipFile(BytesIO(response.body))

        doc = next((name for name in z.namelist() if '.kml' in name), None)
        if doc is None:
            raise ValueError('unable to find the requested resource')

        kml = z.read(doc)
        return func(klass, doc, Selector(text=kml), response.meta)

    return _inner
