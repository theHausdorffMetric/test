"""Collection of helper functions for internationalisation and localisation.

NOTE we may want to switch to a proper library at some point for more robust i18n/l10n support

"""
DUTCH_TO_ENGLISH_MONTHS = {
    'januari': 'january',
    'februari': 'february',
    'febr': 'february',
    'maart': 'march',
    'mrt': 'march',
    'mei': 'may',
    'juni': 'june',
    'juli': 'july',
    'augustus': 'august',
    'oktober': 'october',
    'okt': 'october',
}

FRENCH_TO_ENGLISH_MONTHS = {
    'janvier': 'january',
    'février': 'february',
    'mars': 'march',
    'avril': 'april',
    'mai': 'may',
    'juin': 'june',
    'juillet': 'july',
    'août': 'august',
    'septembre': 'september',
    'octobre': 'october',
    'novembre': 'november',
    'décembre': 'december',
}

SPANISH_TO_ENGLISH_MONTHS = {
    'enero': 'january',
    'febrero': 'february',
    'marzo': 'march',
    'abril': 'april',
    'mayo': 'may',
    'junio': 'june',
    'julio': 'july',
    'agosto': 'august',
    'septiembre': 'september',
    'octubre': 'october',
    'octubure': 'october',
    'noviembre': 'november',
    'diciembre': 'december',
}

PORTUGUESE_TO_ENGLISH_MONTHS = {
    'janeiro': 'january',
    'fevereiro': 'february',
    'marco': 'march',
    'março': 'march',
    'abril': 'april',
    'maio': 'may',
    'junho': 'june',
    'julho': 'july',
    'agosto': 'august',
    'setembro': 'september',
    'outubro': 'october',
    'novembro': 'novemeber',
    'dezembro': 'december',
}


def translate_substrings(foreign_str, translation_dict, delim=' '):
    """Translate strings generically given a translation dictionary.

    The motivation for this helper function came when we were faced with non-English
    dates and non-Latin port names. Because `dateutil` is not locale-aware, we are unable to
    fuzzy match date strings in other languages. Because we need to display romanised port names
    on the platform, we need to match them by romanised names. Hence this helper function.

    Since it is linguistically improbable that a single word can have substrings belonging to
    multiple languages, we use a ' ' delimeter by default to denote the start and end of a foreign
    word to translate.

    NOTE case-sensitive

    Args:
        string (str): string in other language to be translated
        translation_dictionary (Dict[str, str]): mapping of foreign key to romanised value
        delim (str): delimiters in offending string to be translated

    Returns:
        str: translated string if there is a translation mapping, else original string

    Examples:
        >>> translate_substrings('15 Marzo 2018', {'Marzo': 'March', 'Abril': 'April'})
        '15 March 2018'
        >>> translate_substrings('15 MARZO 2018', {'Marzo': 'March', 'Abril': 'April'})
        '15 MARZO 2018'
        >>> translate_substrings('15Marzo2018', {'Marzo': 'March', 'Abril': 'April'})
        '15Marzo2018'
        >>> translate_substrings('abrillantador', {'abril': 'april', 'abrillantador': 'rinse aid'})
        'rinse aid'

    """
    foreign_str_segments = foreign_str.split(delim)
    for idx, segment in enumerate(foreign_str_segments):
        if segment in translation_dict:
            foreign_str_segments[idx] = translation_dict[segment]
    return delim.join(foreign_str_segments)
