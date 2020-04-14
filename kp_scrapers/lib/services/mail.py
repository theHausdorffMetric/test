# -*- coding: utf-8 -*-


"""IMAP mail library, with a salt of Kpler.

Usage (to be moved in doc)
~~~~~

    # note: keys are case-insensitive
    interesting_attrs = [
        'from',
        'delivered-to',
        'date',
        'sender',
        'subject',
        'mailing-list',
        'message-id',
    ]

    with gmail_folder('Reports/Ocean Brokers/Basrah Oil Terminal') as mailbox:
        q_ = Query().unseen().since('02-04-2017').wrap()
        for msg in mailbox.search(q_):
            for key in interesting_attrs:
                print('[ {} ] {}: {}'.format(msg._id, key, msg.envelope[key]))

            print('[ {} ] body: {}'.format(msg._id, msg.body()))
            for piece in msg.attachments():
                print('[ {} ] found attachment: {}'.format(msg._id, piece.get_filename()))

"""

from __future__ import absolute_import
from collections import namedtuple
from contextlib import contextmanager
import datetime as dt
import email
import imaplib
import logging

from kp_scrapers.lib.services.shub import global_settings, validate_settings


GMAIL_IMAP_HOST = 'imap.gmail.com'
GMAIL_PROTOCOL = '(RFC822)'
# TODO add other possible values
GMAIL_STATUS = namedtuple('Status', 'success')('OK')

logger = logging.getLogger(__name__)


@contextmanager
def gmail_folder(mailbox, settings=None):
    """Contextmanager to connect Gmail mailbox.

    This function allows one to customize settings source.
    For example, using a configuration file or environment
    if one doesn't use the library with Scrapy.

    Args:
        mailbox (str): name of folder within account
        settings (Dict[str, str]):

    Yields:
        GmailFolder:

    """
    settings = settings or global_settings()
    # properly crash if the runtime is not properly configured
    validate_settings('GMAIL_USER', 'GMAIL_PASS', settings=settings)

    logger.info(f'Connecting to IMAP server as user "{settings.get("GMAIL_USER")}"...')
    server = imaplib.IMAP4_SSL(GMAIL_IMAP_HOST)
    _execute_or_crash(server.login, settings.get('GMAIL_USER'), settings.get('GMAIL_PASS'))
    logger.info(f'Connected as user "{settings.get("GMAIL_USER")}"')

    logger.info(f'Connecting to mailbox "{mailbox}"...')
    # `imaplib` does not properly quote mailbox string, hence we need to do it ourselves
    # To support multiple spiders scraping the same email, MARK_MAIL_AS_SEEN can be
    # set to False in the spider settings which would set read_only to True
    # so that SEEN flag is not added to the email.
    read_only = not settings['MARK_MAIL_AS_SEEN']
    _execute_or_crash(server.select, mailbox=f'"{mailbox}"', readonly=read_only)
    logger.info(f'Connected to mailbox "{mailbox}"')

    yield GmailFolder(server)

    logger.info('Logging out of IMAP server')
    server.close()
    server.logout()


def _execute_or_crash(fn, *args, **kwargs):
    """Handle non-OK statuses from imap server executions.

    Args:
        fn (callable):

    Raises:
        ConnectionError: if non-OK server response returned

    """
    status, message = fn(*args, **kwargs)
    if status != 'OK':
        raise ConnectionError(message[0])


# TODO save, visualize (based on type)?
class Attachment(object):
    def __init__(self, part):
        self._raw = part

    @property
    def name(self):
        return self._raw.get_filename()

    @property
    def body(self):
        return self._raw.get_payload(decode=True)

    def _detect_type(self, extension):
        # TODO a smarter way based on content or headers
        # TODO more supported exte,sions by xlrd
        return self._raw.get_filename().endswith(extension)

    @property
    def is_spreadsheet(self):
        # TODO a smarter way based on content or headers
        # TODO more supported extensions by xlrd
        return self._detect_type('.xlsx') or self._detect_type('.xls')

    @property
    def is_pdf(self):
        return self._detect_type('.pdf')

    @property
    def is_docx(self):
        return self._detect_type('.docx')

    @property
    def is_zip(self):
        return self._detect_type('.zip')


# TODO __str__
class Mail(object):

    # parts type we don't want to process
    ignore = ['image']

    def __init__(self, uid, raw_msg):
        self.uid = uid
        self.envelope = email.message_from_bytes(raw_msg)

    def _walk(self, selector):
        for part in self.envelope.walk():
            # multipart are just containers, so we skip them
            if part.get_content_maintype() in self.ignore or part.is_multipart():
                continue

            if selector(part):
                yield part

    def body(self, content_type='html'):
        def select_html_body(part):
            return 'text/{}'.format(content_type) in part.get('content-type')

        # only one body possible, force generator output
        return next(self._walk(select_html_body)).get_payload(decode=True)

    def export(self, filename=None, content_type='html'):
        # NOTE should we do the same for attachments?
        filename = filename or (
            '/tmp/' + self.envelope['subject'].replace(' ', '-').lower() + '.' + content_type
        )

        with open(filename, 'w') as fd:
            logger.info('exporting data to fs://{}'.format(filename))
            fd.write(self.body(content_type))

    def attachments(self):
        def select_attachment(part):
            return part.get_filename() and part.get('Content-Disposition')

        return (Attachment(part) for part in self._walk(select_attachment))

    @property
    def headers(self):
        # headers dict-like access is case-insensitive, and curiously enough
        # there are duplicates
        return [k.lower() for k in set(self.envelope.keys())]


class Query(object):
    """Fluent interface to build (state machine)."""

    def __init__(self):
        # TODO make it a set (idempotent methods)
        self.criteria = []

    @staticmethod
    def _format_date(a_date):
        # NOTE what about datetime
        return a_date.strftime('%d-%b-%Y') if isinstance(a_date, dt.date) else a_date

    def _update(self, criterion):
        self.criteria.append(criterion)
        return self

    def since(self, a_date):
        return self._update('(SINCE "{}")'.format(self._format_date(a_date)))

    def before(self, a_date):
        return self._update('(BEFORE "{}")'.format(self._format_date(a_date)))

    def on(self, a_date):
        return self._update('(ON "{}")'.format(self._format_date(a_date)))

    def sent_by(self, who):
        """Because we can't overwrite `from`."""
        return self._update('(FROM "{}")'.format(who))

    def with_subject(self, what):
        return self._update('(SUBJECT "{}")'.format(what))

    def unseen(self):
        return self._update('(UNSEEN)')

    def all(self):
        return self._update('(ALL)')

    def wrap(self):
        query = " ".join(self.criteria or ['ALL'])
        logger.debug('built IMAP query: `{}`'.format(query))
        # NOTE reset criteria?
        return query

    def __str__(self):
        return self.wrap()


class GmailFolder(object):

    CHARSET = None

    def __init__(self, imap_server):
        self.server = imap_server

    def search(self, criteria='(ALL)', last=None):
        # NOTE what's the difference with self.connection.uid?
        status, ids = self.server.search(self.CHARSET, criteria)
        assert status == GMAIL_STATUS.success

        # from most recent to oldest is more intuitive
        uids_list = list(reversed(ids[0].split()))
        for i, uid in enumerate(uids_list):
            status, data = self.server.fetch(uid, GMAIL_PROTOCOL)
            assert status == GMAIL_STATUS.success

            logger.debug('fetching mail with id {}'.format(uid))
            # NOTE I have no idea what is inside the other cells of data, and
            # neither the documentation I think....
            yield Mail(uid, data[0][1])

            if last and i >= last - 1:
                logger.info('Limited to processing at most {} mail(s), terminating'.format(last))
                break

    def folders(self):
        return self.server.list()

    def _flag_mail(self, uid, flag):
        logger.debug("Flagging mail UID {} with \\{}".format(uid, flag))
        return self.server.uid('STORE', uid, '+FLAGS', '(\\{})'.format(flag))

    def mark_seen(self, uid):
        return self._flag_mail(uid, 'Seen')

    def mark_flag(self, uid):
        return self._flag_mail(uid, 'Flagged')
