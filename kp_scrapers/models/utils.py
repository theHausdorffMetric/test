from functools import wraps
from inspect import isgenerator
import logging
from pprint import pformat
from typing import Any, Dict, Optional

import click
from schematics import Model
from schematics.exceptions import DataError, ValidationError

from kp_scrapers.cli.ui import is_terminal
from kp_scrapers.settings.extensions import MAGIC_FIELDS


logger = logging.getLogger(__name__)


def validate_item(
    model: Model, normalize: bool = False, strict: bool = False, log_level: str = 'warning'
) -> Optional[Dict[str, Any]]:
    """Decorator that validates a supplied item dictionary against a Model.

    How to use:
        @validate_item(SpotCharter, normalize=True, strict=True, log_level='error')
        def process_item(raw_item):
            item_dict = map_keys(raw_item, MAPPING)
            return item_dict

    Args:
        model: schematics model to validate a json-compatible dict against
        normalize: return model-normalized item if True, else return original item
        strict: if validation failed, return None if True, else return original item
        log_level: log level of failed validation attempt

    """

    def _outer_wrapper(fn):
        def _validate(item):
            try:
                item_as_model = model(item)
                item_as_model.validate()
                return item_as_model.to_primitive() if normalize else item
            except (DataError, ValidationError) as e:
                # log failed validation attempt
                _fields = '\n'.join(f'{key} : {repr(err)}' for key, err in e.messages.items())
                cfields = click.style(_fields, fg='red', bold=True) if is_terminal() else _fields
                citem = click.style(pformat(item), fg='yellow') if is_terminal() else pformat(item)
                getattr(logger, log_level)(
                    'Item validation failed\n%(item)s\n%(fields)s',
                    {'item': citem, 'fields': cfields},
                )

                # either we want to invalidate the item or just warn about the data quality
                return None if strict else item

        def _validate_generator(generator):
            for item in generator:
                # magicfields may be auto-appended by the runtime if defined in settings
                # workaround to remove them before validating since they are not part of the model
                [item.pop(key, None) for key in MAGIC_FIELDS]
                yield _validate(item)

        @wraps(fn)
        def _wrapper(*args, **kwargs):
            item = fn(*args, **kwargs)
            # sanity check, since scrapy can return None
            if not item:
                return None

            # handle both decorators and functions
            return _validate_generator(item) if isgenerator(item) else _validate(item)

        return _wrapper

    return _outer_wrapper


def filter_item_fields(scrapy_item, raw):
    """Filter to keep only the keys that are valid fields for the scrapy item.

    Args:
        scrapy_item (scrapy.Item):
        raw (dict[str, str]): raw dictionary with keys that may/may not be defined in scrapy_item

    Returns:
        dict[str, str]: dict with items all belonging to scrapy_item

    """
    # 'provider_name' is mandated here since it's not a Scrapy field in VersionedItem
    # TODO to revert fix once we've shifted to all spiders to "parse -> normalize"
    whitelist = set(scrapy_item.fields) | set(['provider_name'])
    return {k: raw[k] for k in raw if k in whitelist}
