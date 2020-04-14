"""Data archiving and compression library."""

import gzip
import io
import re
import zipfile


def gzip_uncompress(filelike, **options):
    """Uncompress a file that has been compressed with gzip format.

    Args:
        filelike (file):
        **options:

    Yields:
        Any:

    """
    yield from _deserializer(gzip.GzipFile(fileobj=filelike), **options)


def zip_uncompress(filelike, files_to_keep, **options):
    """Uncompress a folder that has been compressed with zip format and retrieve

    Args:
        filelike (file): file-like object
        files_to_keep (str): keep only files that have names that match this regex string
        **options: deserialising, encoding options for interpreting a file, if required

    Yields:
        Tuple[str, tuple[Any]]:

    """
    zipobj = zipfile.ZipFile(file=filelike)
    for file_name in zipobj.namelist():
        # skip if file is irrelevant
        if not re.match(files_to_keep, file_name):
            continue

        # extract unzipped file contents
        with zipobj.open(file_name) as unzipped:
            yield file_name, tuple(_deserializer(unzipped, **options))


def _deserializer(fileobj, reader=None, deserialize=None, encoding='utf-8'):
    """Uncompress a folder that has been compressed with zip format and retrieve

    Args:
        filelike (file):
        reader (Callable[str, Any]):
        deserialize (Callable[str, Any]):
        encoding (str): string encoding; defaults to unicode

    Yields:
        Tuple[str, List[Any]]:

    """
    # wrap filelike as a text-mode filelike
    fileobj = io.TextIOWrapper(fileobj, encoding=encoding) if fileobj else fileobj
    reader = reader if reader else lambda x: x.read().splitlines()
    deserialize = deserialize if deserialize else lambda x: x

    for line in reader(fileobj):
        yield deserialize(line)
