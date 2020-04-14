#! /usr/bin/env python

from pprint import pprint as pp

import click

from kp_scrapers.cli import ui
from kp_scrapers.lib.static_data import vessels


@click.command()
@click.option('--cache/--no-cache', default=False)
@click.argument('imo')
def search(cache, imo):
    ui.info(f"loading static fleet (caching: {cache})")
    fleet = vessels(disable_cache=not cache)
    ui.info(f"searching for vessel with imo: {imo}")
    vessel = fleet.get(imo)
    if vessel:
        ui.success("on Kpler radar")
        pp(vessel, indent=4)


if __name__ == '__main__':
    search()
