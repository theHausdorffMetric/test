import click

from kp_scrapers.cli.ui import fail, hr, info, success
from kp_scrapers.cli.utils import walk_configs
from kp_scrapers.commands import run_scrapy_command  # noqa
from kp_scrapers.lib.services import kp_airtable, shub


click.disable_unicode_literals_warning = True

CHECKS = ['scheduling', 'scrapinghub', 'airtable', 'spider']


def load_jobs(jobs_path, blacklist=None):
    for conf in walk_configs(jobs_path, blacklist=blacklist):
        for job in conf.get('jobs'):
            yield job


def lookup(dataset, value, key):
    return value in list(map(lambda x: x.get(key), dataset))


def _should_run(step, checks):
    return (
        step in list(checks)
        # empty checks defaults to run everything
        or not checks
    )


# TODO retrieve last job and check for 0 items, bad finish state (datadog?)
@click.command('doctor')
@click.option(
    '-p',
    '--project',
    'project_id',
    default='production',
    callback=shub.to_project_id,
    help='ScrapingHub environment',
)
@click.option('-J', '--jobs-path', default='./scheduling', help='root jobs def path')
@click.option('-i', '--ignore', multiple=True, default=['Noop'], help='list of spiders to ignore')
@click.option(
    '--check', 'checks', multiple=True, type=click.Choice(CHECKS), help='limit checks to perform'
)
@click.option(
    '-f',
    '--filter',
    'filters',
    multiple=True,
    default=['enabled:true'],
    help='list of spider attributes to filter by. try "scrapy describe" to see more',
)
def doctor(project_id, jobs_path, ignore, checks, filters):
    """Register several periodic jobs from a yaml conf."""
    info("loading local jobs path={}".format(jobs_path))
    local_jobs = list(load_jobs(jobs_path, blacklist=['settings.yml']))

    # TODO merge `sensible` table
    info("loading airtable base table={}".format('Overview'))
    records = list(kp_airtable.retrieve_all_records('Data Sourcing', 'Overview'))

    info("loading Scrapinghub periodic jobs project={}".format(project_id))
    shub_jobs = []
    disabled_jobs = []
    for job in shub.periodic_jobs(project_id).json().get('results', []):
        if not job['disabled']:
            shub_jobs.extend(job.get('spiders'))
        else:
            disabled_jobs.extend(job.get('spiders'))

    filteropts = ''
    for _filter in filters:
        filteropts += '--filter {} '.format(_filter)
    info("filtering spiders by attributes: {}".format(filters))

    for spider in run_scrapy_command('describe', '{} --silent'.format(filteropts)):
        if spider['name'] in ignore:
            info("ignoring spider {}".format(spider['name']))
            continue

        if _should_run('scheduling', checks):
            if not lookup(local_jobs, value=spider['name'], key='spider'):
                fail("spider {name} is not scheduled locally".format(**spider))

        if _should_run('scrapinghub', checks):
            if lookup(disabled_jobs, value=spider['name'], key='name'):
                info("spider {name} is disabled on Scrapinghub".format(**spider))
            elif not lookup(shub_jobs, value=spider['name'], key='name'):
                fail("spider {name} is not scheduled on Scrapinghub".format(**spider))

        if _should_run('airtable', checks) and spider.get('provider'):
            # TODO compare records['State'] with last job run
            if not lookup(records, value=spider['provider'], key='Name'):
                fail("spider {name} is not documented on Airtable".format(**spider))

        if _should_run('spider', checks):
            if not spider['version']:
                fail("spider {name} is not versioned".format(**spider))
            if not spider['provider']:
                fail("spider {name} doesn't define a data provider".format(**spider))
            if not spider['produces']:
                fail("spider {name} doesn't define the data types produced".format(**spider))

        hr('-')

    success("diagnostic done - patient in trouble")
