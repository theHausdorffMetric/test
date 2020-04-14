import itertools
import re


def flatten_table_row(selectors):
    """Flatten a table row that may contain "sub-rows" delimited by linebreaks.

    Args:
        selectors (List[scrapy.Selector]):

    Returns:
        List[str]:

    """
    return list(
        itertools.chain.from_iterable(naive_parse_html(head.extract()) for head in selectors)
    )


def naive_parse_html(html_string, delimiter='<br>'):
    """Naively parse, remove and split element contents by linebreak HTML tag.

    Python's builtin xml library was not used because of the presence of non-closing tags.

    Args:
        html_string (str):

    Returns:
        List[str]:

    Examples:
        >>> naive_parse_html('<span>Voyage #<br>Signal<br>Berth #</span>')
        ['Voyage #', 'Signal', 'Berth #']
        >>> naive_parse_html('<span class="word8c">HONG KONG<br>KEELUNG<br>KAOHSIUNG</span>')
        ['HONG KONG', 'KEELUNG', 'KAOHSIUNG']

    """
    # sanity check, in case we obtain ill-formatted strings
    contents_match = re.match(r'^<[\w\s\"\=\#\:]+>(.*)<\/.+>$', html_string)
    if not contents_match:
        raise ValueError(f'Unexpected HTML string: {html_string}')

    # split by linebreak tags
    return contents_match.group(1).split(delimiter)
