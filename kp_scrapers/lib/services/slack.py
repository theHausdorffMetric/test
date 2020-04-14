# -*- coding: utf-8 -*-

"""Communication channels."""

from __future__ import absolute_import
from collections import namedtuple

from slackclient import SlackClient

from kp_scrapers.lib.services.shub import global_settings as Settings


PROJECT_USER = 'Data Bo(a)t'
# I know we're not on the ETL, but like `ct-pipeline`says:
# "Send all missed alerts in one place in case of missing channel"
# so I believe that is consistent
DEFAULT_CHANNEL = '#etl_bot'

Action = namedtuple('Action', ['type', 'text', 'url'])
SlackUser = namedtuple('SlackUser', ['name', 'uid'])

# copied from kp-admin
SLACK_USERS = {
    'ealfonsi@kpler.com': SlackUser('emma', 'U06D7RSH0'),
    'ebelostrino@kpler.com': SlackUser('emmanuel', 'UDUKUHC7R'),
    'epowell@kpler.com': SlackUser('eli', 'UBR09S3DZ'),
    'hkargar@kpler.com': SlackUser('homa', 'U3V61P1GT'),
    'hwang@kpler.com': SlackUser('hui', 'UBJ8NG2UF'),
    'iniklyaev@kpler.com': SlackUser('ilya', 'U1QED65DE'),
    'jantequera@kpler.com': SlackUser('Jorge', 'U1QEGS2A1'),
    'jjuay-ext@kpler.com': SlackUser('janus', 'UMM4PFX8E'),
    'movergaard@kpler.com': SlackUser('madeleine', 'U06DFN5JM'),
    'mtrivedi@kpler.com': SlackUser('Malay', 'UF82HRP0R'),
    'mveron@kpler.com': SlackUser('matthis', 'U0SEU7P0T'),
    'nleconte@kpler.com': SlackUser('Nathalie', 'UBECNGKM5'),
    'ppater@kpler.com': SlackUser('pierre', 'U1NH23EUX'),
    'qboucly@kpler.com': SlackUser('quentin', 'U5EQCT5QW'),
    'qchen@kpler.com': SlackUser('qiaoling', 'U817BSJ68'),
    'rchia@kpler.com': SlackUser('rebecca', 'U06DFN5JM'),
    'sahmed@kpler.com': SlackUser('Samah', 'UCL7M7Z9T'),
    'skiriukhina@kpler.com': SlackUser('sofia', 'U0WEXCYGJ'),
    'song@kpler.com': SlackUser('szeswee', 'U8N9EDQGN'),
    'sggo@kpler.com': SlackUser('eric', 'UK2DS0D9R'),
    'vlim@kpler.com': SlackUser('viola', 'U8LTJ2J02'),
    'vtan@kpler.com': SlackUser('venice', 'UM7402TD4'),
    'xbruhiere@kpler.com': SlackUser('xavier', 'U48CWQ6NT'),
    'yeu@kpler.com': SlackUser('yining', 'UBQMD4PRA'),
    'ylchay@kpler.com': SlackUser('youliang', 'U2R5246LX'),
    'yrtan@kpler.com': SlackUser('sean', 'U5GJ53F2N'),
    'zliu@kpler.com': SlackUser('ziqi', 'UL7SF8L3T'),
    'zyuan@kpler.com': SlackUser('zhifei', 'U9V6R4Y0Y'),
}


def select_channel():
    """Abstract how we decide which channel to target.

    Useful choices:

        crew: '#crew-process-reports'
        test: '#dev-alerts-stg'
        dev: '#dev-alerts-data'
        engineering: '#crew-data-sourcing'
        analysts: '#sgp-data'

    """
    # use channel defined in project settings
    # overriding channel in Scrapinghub spider settings will not work
    return Settings().get('SLACK_CHANNEL') or DEFAULT_CHANNEL


def send(text, channel=None, attachments=None):
    sc = SlackClient(Settings().get('SLACK_TOKEN'))
    channel = channel or select_channel()

    res = sc.api_call(
        'chat.postMessage',
        channel=channel,
        text=text,
        # overwrite user name (otherwise guess from token)
        username=PROJECT_USER,
        mrkdwn=True,
        attachments=attachments,
        icon_emoji=':mailchimp:',
    )

    return res


def build_table(**kwargs):
    """A less verbose solution to define `fields` in messages."""
    return [{'title': k, 'value': v, 'short': True} for k, v in kwargs.items()]


def build_actions(*slack_actions):
    """A less verbose solution to define `button` in messages."""
    desc = {'fallback': 'http://example.com', 'actions': []}
    for action in slack_actions:
        desc['actions'].append({'type': action.type, 'text': action.text, 'url': action.url})

    return desc


def mention(email):
    """Transform an email into a mention '@someone' for a slack message.

    Examples:
        >>> mention('xbruhiere@kpler.com')
        '<@U48CWQ6NT>'

    """
    return '<@{}>'.format(SLACK_USERS[email].uid)
