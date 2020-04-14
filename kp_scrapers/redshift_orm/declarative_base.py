from sqlalchemy import inspect
from sqlalchemy.ext.declarative import as_declarative


@as_declarative()
class ABase:
    def __init__(self, *args, **kwargs):
        pass

    def to_dict(self):
        return {c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs}
