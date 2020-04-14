import click

from kp_scrapers.cli.ui import fail, info, success
from kp_scrapers.lib.services import shub


BASE_URL = 'https://app.scrapinghub.com'


@click.command('browse')
@click.option(
    '-p',
    '--project',
    'project_id',
    default='production',
    callback=shub.to_project_id,
    help='ScrapingHub environment',
)
@click.argument('spider_name', required=False)
def browse(project_id, spider_name):
    """Output the spider url on scrapinghub."""
    # trick to display env dashboard when no spider are provided
    if spider_name:
        spider = shub.shub_conn().get_project(project_id).spiders.get(spider_name)  # noqa
        spider_url = f"{BASE_URL}/p/{spider.key}"
    else:
        spider_url = f"{BASE_URL}/p/jobs"

    # keep it the only info on stdout so user can wire it to bash tricks
    # example: `> open $(kp-shub browse SpireApi)`
    print(spider_url)


@click.command('check')
@click.option('-c', '--count', default=5, help='limit check to this number of jobs')
@click.option('-E', '--expect', default=0, help='Expected minimum number of items')
@click.option(
    '-p',
    '--project',
    'project_id',
    default='production',
    callback=shub.to_project_id,
    help='ScrapingHub environment',
)
@click.argument('spider_name')
def check(project_id, count, expect, spider_name=None):
    """Inspect spider output and errors."""
    # NOTE inspect project activity when no spider is given?
    spider = shub.shub_conn().get_project(project_id).spiders.get(spider_name)  # noqa
    for job in spider.jobs.iter(count=count):
        msg = f"[ {job['key']} ] {spider_name} scraped {job.get('items', 0)} items"
        if job['close_reason'] != 'finished' or job.get('errors'):
            fail(msg + f" and failed: {job['close_reason']} with {job.get('errors', 0)} errors")
        elif job.get('items', 0) <= expect:
            info(msg)
        else:
            success(msg)
