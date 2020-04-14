# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from collections import deque
from datetime import datetime
from functools import cmp_to_key
import os
import re
import sys
import traceback
import unicodedata

from dateutil import parser
from six.moves import range, zip

from kp_scrapers.lib.utils import compare


def compare_length(x, y):
    """
    returns the sign of the difference of two lengths
    Args:
        x, y(int):
    """
    return compare(len(y), len(x))


def _remove_first_on_the_right(s, to_remove):
    li = s.rsplit(to_remove, 1)
    return ''.join(li)


def _remove_first_on_the_left(s, to_remove):
    li = s.split(to_remove, 1)
    return ''.join(li)


def column_is_optional(column, sep):
    column_type = column[0]
    if sep in column_type:
        column_type, remainder = column[0].split(sep, 1)
    return column_type.endswith('*')


def get_column_type(column, sep):
    column_type = column[0]
    if sep in column_type:
        column_type, remainder = column[0].split(sep, 1)

    if column_type.endswith('*'):
        column_type = column_type[:-1]
    return column_type


class PdfTable(object):
    """
    This is a generic class to parse a text format table.
    It tries to match each field to a column.
    """

    def __init__(self, content):
        self.content = content
        self.lines = content.split('\n')
        self.parse_header()

    def indexed_words(self, line):
        line = line + ' '
        words = []
        indexes = []
        current_word = ''
        for i, c in enumerate(line):
            if c == ' ':
                if current_word != '':
                    words.append(current_word)
                    current_word = ''
            else:
                if current_word == '':
                    indexes.append(i)
                current_word += c
        return list(zip(words, indexes))

    def _find_header_line(self):
        return 0

    def _find_table_end_line(self):
        return None

    def _unzip(self, els):
        return [el[0] for el in els], [el[1] for el in els]

    def parse_header(self):
        self.header_index = self._find_header_line()
        self.table_end_index = self._find_table_end_line()
        self.body = self.lines[self.header_index + 1 : self.table_end_index]
        line = self.lines[self.header_index]
        self.columns, self.column_indexes = self._unzip(self.indexed_words(line))

        # Rename duplicate column names
        counts = {}
        for i, word in enumerate(self.columns):
            if word not in counts:
                counts[word] = 1
            else:
                counts[word] += 1
                self.columns[i] = '{}_{}'.format(word, counts[word])

    def parse(self, smart_distance=True, lower=False):
        for line in self.body:
            processed_line = {}
            for word, index in self.indexed_words(line):
                min_distance = 1000
                nearest_index = -1
                for i, column_index in enumerate(self.column_indexes):
                    distance = abs(index - column_index)
                    simple_distance = distance < min_distance
                    if (not smart_distance and simple_distance) or (
                        smart_distance and simple_distance and index + len(word) >= column_index
                    ):
                        min_distance = distance
                        nearest_index = i
                if nearest_index == -1:
                    continue
                processed_line.setdefault(self.columns[nearest_index], []).append(word)
            if processed_line:
                yield {k.lower(): ' '.join(v) for k, v in processed_line.items()}


def remove_accents(input_str):
    """Given a string returns the same string but without accents.

    This function uses unicode meta data to find accented and
    conbination of normal chars combined with diacritic symbols
    (this may do more than just accents in non-latin laguage).

    Parameters
    ----------

       input_str (str): the string from which remove accents.

    TODO
    ----

       A review of this function by native users of non-latin
       languages might help make sure this function works for
       those languages too.

       Also we should have a module like kpler.normalize for all
       normalization functions and move this function there.
    """
    nkfd_form = unicodedata.normalize('NFKD', u'' + input_str)
    return u"".join([c for c in nkfd_form if not unicodedata.combining(c)])


class ErosionPdfTable(object):
    """A less smart pdf table parsing method

    Requires a bit more knowledge on the content of the table but makes no
    assumptions on the columns alignment (which is a case where the previous
    class fails at parsing some table: when columns overlap).

    The strategy here is: given some knownledge on the table, we can *erode*
    each line by both side, parsing one column at a time. Additionnaly each
    column is typed which helps finding the bounds of each columns.
    """

    _COLUMNS = None
    """A list of column descriptors.

    A column descriptor is a pair:

       column type descriptor, column name.

    A column type descriptor is a string either:

       - 'str' for a string,
       - 'enum' for a enumeration of values, it *must* be followed by
         the list of values each seperated by :attr:`_SEPARATOR`
         (e.g. if _SEPARATOR is '^', 'enum^FIRST^SECOND' is a valdi enum
         descriptor).
       - 'date' for a date, it may be followed by a date template as
         defined in the :meth:`datetime.datetime.strptime`

    If the column type is suffixed with a star ('*'), the column may be
    empty (a mismatch will return None instead of raising a ValueError
    exception)
    """

    _HEADER_STOP = None
    """A list of string to look for to detect the end of the table header
    """

    _START = 'left'
    """On which side start eroding table lines."""

    _SEPARATOR = '+'
    """Char used to seperate a type from its templates in a column descriptior.

    examples::

      ('enum+FIRST+SECOND+FIRST', ...)
      ('date+%Y%m%d', ...)

    """
    _STRATEGY = 'linear'
    """Strategy used to erode the lines in the table.

    :'linear':
         means from left to right if _START is 'left' or the converse
        if _START is 'right'.

    :'alternating':
        means left then right then left and so on, if _START is
        'left'; right then left then right and so on if _START is 'right'
    """

    def __init__(self, content, filename, logger, decimal_sep=','):
        self.logger = logger
        if filename is not None:
            self._filename = os.path.basename(filename)

        if self._COLUMNS is None:
            raise NotImplementedError(
                'You need to derive the Erosion and set ' 'a _COLUMNS attribute.'
            )
        if self._HEADER_STOP is None:
            raise NotImplementedError(
                'You need to derive the Erosion and set ' 'a _HEADER_STOP attribute.'
            )
        self._content = enumerate(content.splitlines())
        self._decimal_separator = decimal_sep
        # On what side of the line are we parsing a column
        self._side = 'left' == self._START
        self._parsed_columns = 0

    @property
    def side(self):
        """Table on which side of the table columns are being eroded.

        Returns
        -------

           The string 'left' if the columns are currently being eroded
           from the left side of the table, or the string 'right' if
           columns are eroded from the right.
        """
        return 'left' if self._side else 'right'

    def _find_first_line(self):
        for self._lineno, self._line in self._content:
            self._line = self._line.strip()
            for stop in self._HEADER_STOP:
                clean_line = remove_accents(self._line)
                if clean_line.endswith(remove_accents(stop)):
                    return
        raise RuntimeError(
            "While parsing '{}', I could not find the position of the first"
            " line in the table.".format(self._filename)
        )

    def _erode(self, matched):
        # if self._side is True:  # Erode on the left hand side
        #     # self._line = self._line[len(matched):]
        # else:
        #     self._line = self._line[:len(matched)]
        # Version below does not assume we are remove data on one of the lines
        # side it simply removed the first occurence of the data match on the
        # side we are currently looking at.
        if self._side is True:
            self._line = _remove_first_on_the_left(self._line, matched)
        else:
            self._line = _remove_first_on_the_right(self._line, matched)

    def _pattern_on_side(self, pattern, discriminating_pattern='.*'):
        """Modifies a regexp pattern to force it to match on one side of a line

        Parameters
        ----------

           pattern (str): is the regexp to modify so it matched only on one
               side of a line.
           discrimination_pattern (str): is an helper pattern you can use to
               prevent the greediness of the pattern matcher from eating parts
               of your match. It is most useful when matching on the right
               side of a line. You may also want to have a look at non-greedy
               rexexp in the :mod:`re` module documentation.

        Returns
        -------

           The modified modified pattern as a string.
        """
        # For regexp to look on one side using either ^ or $
        if self._side is True:  # Look on the left side
            return '^({})(?:{}.*)?$'.format(pattern, discriminating_pattern)
        else:
            return '^(?:.*{})?({})$'.format(discriminating_pattern, pattern)

    def get_columns(self):
        return self._COLUMNS

    def get_strategy(self):
        raise NotImplementedError('PdfTable.')

    def _linear_strategy(self):
        """Strategy that erodes the columns from left-to-rigth or right-to-left

        Actual direction used is dertermined by the _START attribute
        """
        columns = self._line_columns = self.get_columns()
        if self._START == 'right':
            columns = reversed(columns)

        result = {}
        for idx, column in enumerate(columns):
            result[column[1]] = self._parse_column(
                idx if self._side else len(self._line_columns) - idx - 1
            )
        return result

    def _alternating_strategy(self):
        d = deque(self.get_columns())
        nxt = deque.popleft if 'left' == self._START else deque.pop
        result = {}
        for column in nxt(d):
            result[column[1]] = self._parse_column(column)
            nxt = deque.pop if nxt != deque.pop else deque.popleft
        return result

    def _explicit_strategy(self):
        '''Untested.
        '''
        column_indexes = self.get_strategy()

        result = {}
        for column_index in column_indexes:
            result[column_index[1]] = self._parse_column(column_index)
        return result

    def _gap_heuristic(self):
        """Uses a heuristic based on gap made of several blanks to split columns
        """
        for i in range(5, 0, -1):
            res = [x for x in re.split('\W' * i, self._line) if '' != x]
            res_length = len(res)
            if res_length == len(self._line_columns) - self._parsed_columns:
                break
        if i >= 2:
            match = res[0 if self._side else 1]
            self._erode(match.strip())

    def _try_reverse_match_on_removed_bit(self, bit, next_col_idx):
        old_side = self._side
        old_line = self._line
        self._side = not self._side
        self._line = bit

        # Not erode=True, possible because we replaced the current line
        # (self._line) by just the bit of text we are interested in.
        res = self._parse_column(next_col_idx, True)

        # Because we pass erode=True, the number of parsed columns is
        # incremented, which is wrong, so we fix this here...
        self._parsed_columns -= 1

        not_matched = self._line  # Part of the string that wasn't matched
        self._line = old_line
        self._side = old_side
        return res, not_matched

    def _try_with_next_column(self, next_col_idx, saved_line):
        """
        Side effects
        ------------

        Consumes self._line
        """
        bits = self._line.split()
        if self._side is False:
            # Read elements from right to left since we want to match
            # on the right hand side of the line.
            bits.reverse()

        is_optional = column_is_optional(self._line_columns[next_col_idx], self._SEPARATOR)
        # xfix is prefix is self._side is True, suffix other wise.
        for bit_idx, xfix in enumerate(bits):
            saved_line = self._line
            self._erode(xfix)
            try:
                res = self._parse_column(next_col_idx, False)
                if res is None:
                    # Maybe the next column is joined with the first one
                    # try to match it on the opposite side.
                    res, not_matched = self._try_reverse_match_on_removed_bit(xfix, next_col_idx)
                    if res:
                        self._line = saved_line
                        # Put back the bit that reversed matched.
                        # self._line = '{} {}'.format(self._line, xfix)
                        # Remove the part of the reverse match that didn't
                        # match it is part of the string column.
                        self._erode(not_matched)
                if res is None and is_optional is True:
                    continue

                # We matched so we can stop
                break

            except ValueError:
                # Go to next iteration and try to match further on the
                # left or right
                continue

        if '' == self._line:
            # We consumed the whole line without matching
            tried_column = self._line_columns[next_col_idx]
            raise ValueError(
                "Could not match a {} next to a "
                "string on the {} side on line: "
                "\"{}\".".format(
                    get_column_type(tried_column, self._SEPARATOR), self.side, saved_line
                )
            )

    def _parse_str(self, col_idx):
        # Strings are hard because they are of arbitrary length and content
        # if the next column is not a string we use it to better delimit the
        # string itself

        if self._side is True:  # We look at a column on the left hand side
            rnge = list(range(col_idx + 1, len(self._line_columns)))
        else:
            rnge = list(range(col_idx - 1, -1, -1))

        visited = False
        saved_line = self._line

        # Try next columns, we try several because some may be optional
        for next_col_idx in rnge:
            visited = True

            if self._line_columns[next_col_idx][0].startswith('str'):

                if 2 == len(self._line_columns) - self._parsed_columns:
                    # Only two strings remaining try some heuristics based on
                    # the gap between the columns
                    self._gap_heuristic()
                    break

                raise RuntimeError(
                    'I cannot yet cope with two strings next to'
                    ' each other or seperated by optional '
                    'columns in the definition of a table'
                    ' columns'
                )
            try:
                self._try_with_next_column(next_col_idx, saved_line)
                break
            except ValueError:
                # The column we tried is optional we give another shot to the
                # next one.
                if column_is_optional(self._line_columns[next_col_idx], self._SEPARATOR):
                    # A value error is raised by :meth:`._try_with_next_column`
                    # iff the whole has been consumed.
                    # We need to restore it for the next round.
                    self._line = saved_line
                    continue
                else:
                    self._gap_heuristic()

                # Column we tried is not optional it should have matched!
                # We give up.
                raise

        if not visited:
            # Last columns return all the remainder
            return self._line.strip()

        if '' == self._line:
            self._line = saved_line
            raise ValueError(
                'Could not match string on the {} side of line: '
                ' "{}"'.format(self.side, self._line)
            )

        # Compute the part of the line we matched as a string
        if self._side is True:
            match = saved_line[0 : len(saved_line) - len(self._line)]
        else:
            match = saved_line[len(self._line) - len(saved_line) :]
        self._line = saved_line
        return match.strip()

    def _parse_date(self, col_idx):
        type_, date_formats = self._line_columns[col_idx][0].split(self._SEPARATOR, 1)
        accept_none = type_.endswith('*')

        self.logger.debug(
            'Looking for a date on the {} side of line: "{}"'.format(self.side, self._line)
        )
        for date_format in date_formats.split(self._SEPARATOR):
            date_regexp = date_format * 1
            for pattern in ['%d', '%H', '%M', '%m', '%y', '%S']:
                if pattern in date_regexp:
                    date_regexp = date_regexp.replace(pattern, '\d{2}')

            if '%I' in date_regexp:
                date_regexp = date_regexp.replace('%I', '(1(0|1|2)|[1-9])')

            if '%Y' in date_regexp:
                date_regexp = date_regexp.replace('%Y', '\d{4}')

            self.logger.debug(
                'Trying to find a date w. the patterns: {} -- {}'
                ' on the {} side of line: "{}"'.format(
                    date_format, date_regexp, self.side, self._line
                )
            )
            date_regexp = self._pattern_on_side(date_regexp)
            res = re.search(date_regexp, self._line)

            if res is not None:
                break  # Found a match

        if res is None:
            self.logger.info('Date found!')
            if accept_none is False:
                raise ValueError(
                    'Could not find a date on the {} side of'
                    ' line: "{}"'.format(self.side, self._line)
                )
            else:
                return None, ''

        try:
            date = datetime.strptime(res.groups()[0], date_format)
        except ValueError:
            date = parser.parse(res.groups()[0])

        return date, res.groups()[0]

    def _parse_enum(self, col_idx):
        # TODO: maybe allow something other than ':' as a separator.
        enum_items = self._line_columns[col_idx][0].split(self._SEPARATOR)
        type_ = enum_items[0]
        enum_items = enum_items[1:]
        accept_none = type_.endswith('*')

        # Sort items by decreasing length may solve some item n is
        # prefix/suffix of item n+k (k>0) issues.  Prefix/suffix
        # position depends on the side of the line we look at.
        sorted(enum_items, key=cmp_to_key(compare_length))

        for item in enum_items:
            item_regexp = self._pattern_on_side(item, discriminating_pattern=' ')
            res = re.search(item_regexp, self._line)
            if res is not None:
                return item

        if accept_none is True:
            return None

        raise ValueError(
            'Could not parse for an item in enumeration ({})'
            ' on the {} side of the line: "{}".'.format(
                ', '.join(enum_items), self.side, self._line
            )
        )

    def _parse_float(self, col_idx):
        # Beware it's a latin coma ',' not a saxon dot '.'
        pattern = '\d+{}\d+'.format(self._decimal_separator)
        accept_none = self._line_columns[col_idx][0].endswith('*')

        float_regexp = self._pattern_on_side(pattern, discriminating_pattern='[^0-9]')
        res = re.search(float_regexp, self._line)
        if res is None:
            if accept_none is True:
                return None, ''
        else:
            return float(res.groups()[0].replace(',', '.')), res.groups()[0]
        raise ValueError(
            'Could not parse a float (w. pattern {}) on the {}'
            ' side of the line: "{}".'.format(float_regexp, self.side, self._line)
        )

    def _parse_regexp(self, col_idx):
        raise NotImplementedError()

        # Not sure yet how the matched strings can be 'eroded'.
        self._line = self._line.strip()
        type_, regexp = self._line_columns[col_idx][0].split(self._SEPARATOR, 1)
        accept_none = type_.endswith('*')

        regexp = self._pattern_on_side(regexp)
        res = re.search(regexp, self._line)

        if res is None:
            if accept_none is True:
                return None, ''
            raise ValueError(
                "Could not parse line for regular expression `{}'"
                " on its {} side: \"{}\".".format(regexp, self.side, self._line)
            )

        # FIXME: What is the matched string ?
        return res.groups(), ''

    def _parse_int(self, col_idx):
        self._line = self._line.strip()
        accept_none = self._line_columns[col_idx][0].endswith('*')
        int_regexp = '\d+'
        int_regexp = self._pattern_on_side(int_regexp, discriminating_pattern='[^0-9]')
        res = re.search(int_regexp, self._line)

        if res is None:
            if accept_none is True:
                return None, ''
            raise ValueError(
                "Could not parse line for an integer "
                "(w. pattern `{}') on its {} side: \"{}\".".format(
                    int_regexp, self.side, self._line
                )
            )
        return int(res.groups()[0]), res.groups()[0]

    def _parse_column(self, col_idx, erode=True):
        self._line = self._line.strip()  # Quick sanitization

        if self._line_columns[col_idx][0].startswith('char'):
            res = self._parse_char(col_idx)
            matched = res

        elif self._line_columns[col_idx][0].startswith('int'):
            res, matched = self._parse_int(col_idx)

        elif self._line_columns[col_idx][0].startswith('float'):
            res, matched = self._parse_float(col_idx)

        elif self._line_columns[col_idx][0].startswith('enum'):
            res = self._parse_enum(col_idx)
            matched = res

        elif self._line_columns[col_idx][0].startswith('date'):
            res, matched = self._parse_date(col_idx)

        elif self._line_columns[col_idx][0].startswith('str'):
            matched = self._parse_str(col_idx)
            res = matched

        else:
            raise RuntimeError(
                "Unknown column type: `{}'.".format(
                    self._COLUMNS[col_idx][0].split(self._SEPARATOR)[0]
                )
            )

        if erode is True:
            # If erode is False it is because we were parsing a string column
            # and were just visiting the column next to it. So we must *not*
            # increment the number of parsed column
            self._parsed_columns += 1

            if '' != matched:
                self._erode(matched)

        return res

    def _parse_line(self):
        self._parsed_columns = 0  # Reset the column counter
        self._line = self._line.strip()
        pristine_line = self._line

        if '' == self._line:
            return None
        try:
            if 'alternating' == self._STRATEGY:
                result = self._alternating_strategy()
            elif 'linear' == self._STRATEGY:
                result = self._linear_strategy()
        except (RuntimeError, ValueError):
            self.logger.error(
                '{}:{}: could not parse line (see exception '
                'below): "{}"'.format(self._filename, self._lineno, pristine_line)
            )
            exc_type, exc, tb = sys.exc_info()
            exc = traceback.format_exception(exc_type, exc, tb)
            self.logger.error(''.join(exc))
            return None

        # Sanitize the output
        if None in result:
            del result[None]

        return result

    def parse(self):
        self._find_first_line()

        for self._lineno, self._line in self._content:
            d = self._parse_line()

            if d is None:  # An error occured while parsing the line.
                continue

            yield d
