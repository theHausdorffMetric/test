# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from xml.etree.ElementTree import XML
import zipfile


WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'


def read_docx_io(docx):
    """Convert docx to generator yielding rows (str).

    Takes docx(file obj), unzips it and parses the raw XML into
    usable rows to obtain vessel info.

    If you're using mail spiders, example:

    from tempfile import TemporaryFile
    for attachment in mail.attachments():
            f = TemporaryFile()
            f.write(attachment.body)
            for row in enumerate extract_docx(f):
                # parse the row
                yield EtaEvent, BerthedEvent, etc

    Args:
        docx (file): file-like object of docx

    Yields:
        List[str]: row of the docx

    """
    document = zipfile.ZipFile(docx)
    xml_content = document.read('word/document.xml')
    document.close()

    # direct descendants are lines in the original docx
    for element in XML(xml_content).iter(tag=PARA):
        yield ' '.join(part for part in element.itertext())
