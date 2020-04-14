"""Notify slack channels on relevant events."""

import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured

from kp_scrapers.cli.ui import ERROR_COLOR, SUCCESS_COLOR
from kp_scrapers.lib.services import shub, slack
from kp_scrapers.lib.utils import response_to_dict


logger = logging.getLogger(__name__)


class NotifyMiddleware(object):
    """Notify configured channel of job output.

    The extension depends on the Slack service so make sure to configure:

            `SLACK_TOKEN` (mandatory)
            `SLACK_CHANNEL` (optional)

    In addition you can customize the following parameters:

            `NOTIFY_ENABLED` - activate the extension
            `NOTIFY_SOMEONE` - ping analyst on Slack, separated by `;`
            `NOTIFY_DEV_IN_CHARGE` - ping technical maintainer of the scraping project
            `NOTIFY_ON_NO_DATA` - prevent messages to be sent if no data AND no errors

    """

    def __init__(self, settings):
        self._failure = None
        self.notify_on_no_items = settings.get('NOTIFY_ON_NO_DATA')
        self.analyst_in_charge = settings.get('NOTIFY_SOMEONE')
        self.dev_in_charge = settings.get('NOTIFY_DEV_IN_CHARGE')

    @classmethod
    def from_crawler(cls, crawler):
        if str(crawler.settings.get('NOTIFY_ENABLED')) != 'True':
            raise NotConfigured('Slack extension is disabled')
        if not crawler.settings.get('SLACK_TOKEN'):
            raise NotConfigured("no api token found")

        ext = cls(crawler.settings)

        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.spider_error, signal=signals.spider_error)

        return ext

    @staticmethod
    def _notify(msg, **kwargs):
        res = slack.send(msg, **kwargs)
        if not res.get('ok'):
            logger.error(f'failed to notify: {res.get("error")}')

    def spider_closed(self, spider):
        items_count = spider.crawler.stats.get_value('item_scraped_count')
        errors_count = spider.crawler.stats.get_value('log_count/ERROR')
        missing_rows = getattr(spider, 'missing_rows', [])

        no_items = not items_count and not errors_count
        if no_items and not self.notify_on_no_items and not missing_rows:
            logger.info('no data nor errors, not sending notification')
            return

        logger.info(f'slacking message to channel {slack.select_channel()}')
        stats = {
            'title': 'Extraction stats',
            'fields': slack.build_table(
                Items=items_count or 0, Errors=errors_count or 0, ID=spider.job_name
            ),
            'color': ERROR_COLOR if self._failure else SUCCESS_COLOR,
        }

        job_url = shub.spider_job_url(spider.job_name)
        logger.debug('linking to project {}'.format(job_url))
        job_button = slack.Action('button', '🔎 Job', job_url)

        if hasattr(spider, 'job_items_url'):
            items_button = slack.Action('button', ':secret:️ Data', spider.job_items_url)
        else:
            items_button = slack.Action('button', ':secret:️ Data', job_url + '/items')

        if self._failure:
            msg = f'Spider *{spider.name}* failed ❗️'
            if self.dev_in_charge:
                dev = slack.mention(self.dev_in_charge)
                msg += f'\n{dev} you should look into it :hammer_and_wrench:'

        else:
            msg = f'Spider *{spider.name}* is done :_ok:'

            if self.analyst_in_charge:
                msg += '\n'
                for analyst in self.analyst_in_charge.split(';'):
                    analyst = analyst.strip()
                    if not analyst:
                        continue

                    msg += f'{slack.mention(analyst)} '

                msg += 'you might want to take a look :eyes:'

            if missing_rows:
                msg += '\nPossible missing rows, you might need to key in manually 🚨️ \n'
                msg += '```'
                for row in missing_rows:
                    msg += row
                    msg += '\n'
                msg += '```'

        self._notify(
            msg=msg,
            attachments=[
                stats,
                # commented out to protect source confidentiality
                # self._failure,
                slack.build_actions(job_button, items_button),
            ],
        )

    def spider_error(self, failure, response, spider, signal=None, *args, **kwargs):
        """Record failure, message will be sent at the end."""
        data = response_to_dict(response)
        self._failure = {
            'title': 'Faulty http request details',
            'fields': slack.build_table(Status=data.get('status'), URL=data.get('url')),
            'color': ERROR_COLOR,
        }
