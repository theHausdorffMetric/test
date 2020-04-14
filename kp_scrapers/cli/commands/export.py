# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import csv
import json

import click

from kp_scrapers.cli.ui import fail, info, success
from kp_scrapers.cli.utils import fetch_jobs, has_scraped_data, search_opts
from kp_scrapers.lib.services import shub


DEFAULT_MARKERS = ('exported',)


def default_session_id():
    """Timestamp based session id one can use for marker and exports."""
    from kp_scrapers.cli.project import git_head_branch
    import time

    return '{commit}_{epoch}'.format(commit=git_head_branch('short'), epoch=int(time.time()))


def noop(row):
    yield row


# TODO define and dynamically use.load mappers
def flatten_equasis(item):
    yield {
        'type': item.get('type', 'n/a'),
        'build_year': item.get('build_year', 'n/a'),
        'call_sign': item.get('call_sign', 'n/a'),
        'dead_weight': item.get('dead_weight', 'n/a'),
        'flag': item.get('flag_name', 'n/a'),
        'gross_tonnage': item.get('gross_tonnage', 'n/a'),
        'imo': item['imo'],
        'mmsi': item.get('mmsi', 'n/a'),
        'name': item.get('name', 'n/a'),
        'status': item.get('status', 'n/a'),
        'status_date': item.get('status_date', 'n/a'),
        'updated_at': item.get('updated_time', 'n/a'),
    }


def equasis_players(item):
    for company in item['companies']:
        yield {
            # common stuff
            'vessel.year': item.get('build_year'),
            'vessel.callsign': item.get('call_sign'),
            'vessel.dwt': item.get('dead_weight'),
            'vessel.flag': item.get('flag_name'),
            'vessel.gt': item.get('gross_tonnage'),
            'vessel.imo': item.get('imo'),
            'vessel.name': item.get('name'),
            'vessel.status': item.get('status'),
            'vessel.status_date': item.get('status_date'),
            # company info
            'company.address': company.get('address'),
            'company.role': company.get('role'),
            'company.name': company.get('name'),
            'company.imo': company.get('imo'),
            'company.date_of_effect': company.get('date_of_effect'),
        }


def encode_korean(row):
    row['dischargePort'] = row['dischargePort'].encode('utf8')
    row['shipAgent'] = row['shipAgent'].encode('utf8')
    row['loadPort'] = row['loadPort'].encode('utf8')

    return row


TRANSFORMERS = {
    'equasis': flatten_equasis,
    'players': equasis_players,
    'korean': encode_korean,
    'noop': noop,
}


def csv_export(data, fd):
    header = list(data[0].keys())
    writer = csv.DictWriter(fd, fieldnames=header, delimiter=';')
    writer.writeheader()
    for row in data:
        # writer.writerow({k: str(v) for k, v in row.items()})
        writer.writerow(row)


def jl_export(data, fd):
    for row in data:
        fd.write(json.dumps(row) + '\n')


@click.command()
@search_opts
@click.option('-t', '--transform', type=click.Choice(TRANSFORMERS.keys()), default=None)
@click.option('-o', '--output', default='export.csv', help='dump data into FILE')
@click.option('-m', '--marker', 'markers', multiple=True, help="list of tags to add to the job")
@click.option('-D', '--deduplicate-on', help="odler job limit to process")
@click.option('-F', '--filter-file', help="only process item present in this csv")
@click.option('-S', '--session-id', default=default_session_id())
@click.option('-r', '--retry', help="resume session with the given id")
def export(
    spider, transform, output, markers, deduplicate_on, filter_file, session_id, retry, **opts
):
    """Export Scrapinghub items."""
    if retry:
        # TODO append to session file?
        info("resuming session `{}`".format(retry))
        session_id = retry
        opts['skip_tags'].append(session_id)
    else:
        info('starting session `{}`'.format(session_id))

    transform = TRANSFORMERS.get(transform) or noop
    markers = list(markers + DEFAULT_MARKERS + (session_id,))
    raw_data = []
    unique_keys = []
    fails = 0

    constraint_keys = []
    if filter_file:
        info("loading constraints file {}".format(filter_file))
        with open(filter_file) as csvfile:
            reader = csv.DictReader(csvfile)
            constraint_keys = [row[deduplicate_on] for row in reader]

    opts['spider'] = spider
    for job in filter(has_scraped_data, fetch_jobs(**opts)):
        try:
            # tag it to remmeber we processed it (ETL style)
            shub.update_tags(job, add=markers)

            info("processing job {}".format(job.key))
            for item in job.items.iter():
                if deduplicate_on:
                    if item[deduplicate_on] not in unique_keys:
                        if constraint_keys:
                            if item[deduplicate_on] in constraint_keys:
                                # indivual items can be nested and store multiple types
                                # so `transform` is a generic generator we consume here
                                raw_data.extend([partial for partial in transform(item)])
                                unique_keys.append(item[deduplicate_on])
                        else:
                            raw_data.extend([partial for partial in transform(item)])
                            unique_keys.append(item[deduplicate_on])
                else:
                    raw_data.extend([partial for partial in transform(item)])
        except Exception as e:
            fail('fetching jobs crashed: {}, going on'.format(e))
            # dumping the data we got so far
            # one might be able to resume execution
            # NOTE could dump data so far in output = '/tmp/' + session_id + '.' + output
            fails += 1

    success("done ({} exceptions), exporting data".format(fails))

    if raw_data:
        output_format = output.split('.')[-1]
        success('exporting data raw={} to {}'.format(len(raw_data), output))

        # TODO support sqlite
        # TODO support stdout
        # TODO support gdrive export - might be better to wait for anbother PR bringing
        #      kp-gdrive and its high-level interface
        with open(output, 'w') as fd:
            if output_format == 'csv':
                csv_export(raw_data, fd)
            elif output_format == 'jl':
                jl_export(raw_data, fd)
    else:
        fail("no data was fetched")
