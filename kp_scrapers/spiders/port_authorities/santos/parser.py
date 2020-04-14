import re


def remove_td_tags(html):
    """Remove HTML tags from string.

    Specifically, remove only <td> opening/closing tags that may or may not contain attributes.
    Although this function is merely one-line long, the examples given here should illustrate how
    the string replacing works.

    This source provides vessel length/draft, cargo movement, and cargo products in the same cell,
    delineated by the <br> tag. Scrapy's default selectors will remove the <br> tags and concatenate
    them without any delimiter.

    This function is required to ensure the <td> tags are properly removed with a actual delimiter.

    Args:
        html (str | None): contains <td> tags

    Returns:
        str | None:

    Examples:
        >>> remove_td_tags('<td align="center">229<br>13</td>')
        '229<br>13'
        >>> remove_td_tags('<td>PANAMENHA</td>')
        'PANAMENHA'
        >>> remove_td_tags('<td align="center">17/03/2018 20:00:00</td>')
        '17/03/2018 20:00:00'
        >>> remove_td_tags('<td align="center">PELLETS A GRANEL<br></td>')
        'PELLETS A GRANEL<br>'
        >>> remove_td_tags('<td align="center"><br>PELLETS A GRANEL</td>')
        '<br>PELLETS A GRANEL'
        >>> remove_td_tags('<td><br>PELLETS A GRANEL</td>')
        '<br>PELLETS A GRANEL'
        >>> remove_td_tags('Santos')
        'Santos'
        >>> remove_td_tags('')

    """
    return re.sub(r'<\/?td(.*")?>', '', html) if html else None
