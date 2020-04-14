# -*- coding: utf-8 -*-

"""Scraper base for data sent by mail.

Its goal is, as one can expect from bases, to abstract email access and only
requires a callback executed for every mails researched.
Note that Scrapy mechanism is useless and we only requests an arbitrary page to
get the spider running.

Implementation
~~~~~~~~~~~~~~

An example sometimes worths a thousand words:

        from kp_scrapers.spiders.bases.mail import MailSpider


        class MailReportSpider(MailSpider):

            name = 'foo'
            version = '0.1.0'
            provider = 'unknown'

            folder = 'Reports/Ocean Brokers/Basrah Oil Terminal'

            def parse_mail(self, mail):
                '''The method will be called for every mail the search_term matched.'''

                # transform raw html to the Selector structure, easier to parse
                body = self.select_body(mail)
                yield body.xpath('...').extract_first()

                for doc in mail.attachments():
                    yield doc.get_payload(decode=True)

"""

from __future__ import absolute_import, unicode_literals
from abc import ABCMeta, abstractmethod

from scrapy import Request, Spider
from scrapy.http import HtmlResponse, Response
import six

from kp_scrapers.constants import BLANK_START_URL
from kp_scrapers.lib.services.mail import gmail_folder


class MailSpider(six.with_metaclass(ABCMeta, Spider)):
    start_urls = [BLANK_START_URL]

    def __init__(self, folder, query='(ALL)', limit=1):
        """Init MailSpider.

        Args:
            folder (str): name of folder as depicted on GMail web interface
            query (str): IMAP query string
            limit (str | int): limit number of mails processed to this amount

        """
        self.folder = folder
        self.query = query
        self.limit = int(limit)

    def parse(self, _):
        """Search and iterate over results.
        """
        with gmail_folder(self.folder, settings=self.settings) as mailbox:
            self.logger.info('Searching for mails `{}` in {} ...'.format(self.query, self.folder))

            # hide not intuitive `.wrap()` from user API
            for msg in mailbox.search(criteria=self.query, last=self.limit):
                self.logger.info(
                    'Mail header:'
                    '\nFROM: {}\nDATE: {}\nSUBJECT: {}'.format(
                        msg.envelope['from'], msg.envelope['date'], msg.envelope['SUBJECT']
                    )
                )

                # transform dict to csv that analysts will work with and vet results
                for item in self.parse_mail(msg) or []:
                    yield item
                self.logger.info('Finished processing mail UID {}'.format(msg.uid))

    @abstractmethod
    def parse_mail(self, mail):
        """Method for parsing an individual mail message.

        You must override this method when inheriting from this base class.

        Args:
            mail (Mail): see `lib.services.api.Mail` for details

        Yields:
            Dict[str, str]: dictionary of an event to be inserted as a Google Sheet row

        """
        pass

    def select_body_html(self, mail):
        """Syntax sugar to hide from implementation the transformation detail."""
        return HtmlResponse(
            url=BLANK_START_URL, body=mail.body(), request=Request(url=BLANK_START_URL)
        )

    def select_body_attachment(self, attachment):
        """Syntax sugar to hide from implementation the transformation detail."""
        return Response(
            url=BLANK_START_URL, body=attachment.body, request=Request(url=BLANK_START_URL)
        )
