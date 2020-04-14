# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
from datetime import datetime, timedelta
import re

from pytz import timezone, utc
from six.moves import range

from kp_scrapers.lib.parser import try_apply
from kp_scrapers.models.items import ArrivedEvent, Cargo, EtaEvent, VesselIdentification


local_timezone = timezone('America/Mazatlan')
# Pattern used to read product description
# Problematic B/M can be found in description, where / is used to discriminate
# between load and discharge. This pattern will be removed if exists
substitute_B_M_pattern = re.compile(re.escape('B/'), re.IGNORECASE)
discharge_pattern = re.compile('descarga( de | del | )*|DESEMBARQUE( de | del | )*', re.IGNORECASE)
load_pattern = re.compile('^carga( de | del | )|EMBARQUE( de | del | )', re.IGNORECASE)
empty_or_transit_pattern = re.compile('LASTRE|TRANSITO|TR\xc1NSITO', re.IGNORECASE)
go_to_fish_pattern = re.compile('LA PESCA', re.IGNORECASE)


def validate_header(received_header, expected_header):
    if len(received_header) != len(expected_header):
        return False
    for i in range(len(expected_header)):
        cleaned_header = received_header[i].split('</th>')[0]
        if cleaned_header != expected_header[i]:
            return False
    return True


def get_viaje_number(viaje_cell):
    try:
        expr = re.search('[vV]iaje=[0-9]*', viaje_cell).group()
        return expr.split('=')[1]
    except Exception:
        return None


def extract_utc_time(realized_date_cell, add_12_hours=False):
    try:
        expr = re.search(
            '[0-9]{2}/[0-9]{2}/20[0-9]{2} [0-9]{2}:[0-9]{2}', realized_date_cell
        ).group()
        realized_date = datetime.strptime(expr, '%d/%m/%Y %H:%M')
        if add_12_hours:
            realized_date += timedelta(hours=12)
        realized_date_with_tz = local_timezone.localize(realized_date)

        return realized_date_with_tz.astimezone(utc).isoformat()
    except Exception:
        return None


def primary_row_parser(row, expected_header):
    """parse line from either Programados or Historico pages.

    Note that there is a second type of pages that can be scrapped, that are
    more detailled information on a voyage - viaje

    """
    row = re.sub(re.escape('<td>'), '', row)
    split_line = row.split('</td>')
    # Build correct Event
    # Next try/except aims at handling both Programados and Historico pages
    # whose headers are slightly differents
    try:
        # Programados page case
        date = split_line[expected_header.index('ETA')]
        hour = split_line[expected_header.index('HORA')]
        eta_date_cell = date + " " + hour
        destination = split_line[expected_header.index('DESTINO')]
    except ValueError:
        # Historico page case
        eta_date_cell = split_line[expected_header.index('ATRAQUE')]
        # In this case, destination will be added later from detail page
        destination = None

    realized_date_cell = split_line[expected_header.index('FONDEO')]

    origin = split_line[expected_header.index('ORIGEN')]

    realized_date = extract_utc_time(realized_date_cell)
    if realized_date:
        item = ArrivedEvent(arrival=realized_date, matching_date=realized_date)

        # NOTE we don't yield foreign ETA due to a bug on the ETL
        # item['previous_zone'] = origin
    else:
        eta_format = extract_utc_time(eta_date_cell)
        item = EtaEvent(eta=eta_format, matching_date=eta_format)
        # Next line is commented as it caused a bug : when specified, 'next_zone'
        # field replaces 'port_name'
        # item['next_zone'] = destination

    item['berth'] = split_line[expected_header.index('MUELLE')]
    item['shipping_agent'] = split_line[expected_header.index('NAVIERA')]

    info_from_detail_page = dict()
    # Fill Vessel informations
    info_from_detail_page['vessel_name'] = split_line[expected_header.index('BUQUE')]
    info_from_detail_page['vessel_flag'] = split_line[expected_header.index('PAIS')]
    info_from_detail_page['vessel_length'] = split_line[expected_header.index('ESLORA')]

    # Get viaje ID and then go to another page to get MMSI and ports
    try:
        # Programados page case
        info_from_detail_page['viaje_number'] = get_viaje_number(
            split_line[expected_header.index('VID')]
        )

        info_from_detail_page['vessel_width'] = split_line[expected_header.index('MANGA')]
        info_from_detail_page['product'] = split_line[expected_header.index('DESC. CARGA')]
        info_from_detail_page['quantity'] = split_line[expected_header.index('TONELADAS')]
    except ValueError:
        # Historico page case
        info_from_detail_page['viaje_number'] = get_viaje_number(
            split_line[expected_header.index('ID')]
        )
        info_from_detail_page['vessel_width'] = None
        info_from_detail_page['product'] = split_line[expected_header.index('CARGA')]
        info_from_detail_page['quantity'] = split_line[expected_header.index('TONS')]

    # Fill information for Cargo
    info_from_detail_page['origin'] = origin
    info_from_detail_page['destination'] = destination

    return (item, info_from_detail_page)


def return_data_if_correct_field(line, field):
    """
    This function expects to have in arguments a string which respects
    format *KEY</td><td>VALUE</td>*
    It returns the VALUE if KEY match requested field
    """
    field_length = len(field)
    field_cell, value_cell = line.split('</td><td>')
    if field_cell[-field_length:] == field:
        return value_cell.split('</td>')[0]
    return None


def skip_cargo(one_cargo):
    return (
        empty_or_transit_pattern.search(one_cargo) is not None
        or go_to_fish_pattern.search(one_cargo) is not None
    )


def identify_move(one_cargo):
    if discharge_pattern.search(one_cargo) is not None:
        return 'discharge', discharge_pattern.sub('', one_cargo)
    elif load_pattern.search(one_cargo) is not None:
        return 'load', load_pattern.sub('', one_cargo)
    else:
        # Type of move not found, return whole cargo description
        return None, one_cargo


def identify_single_move_from_quantities(quantity_unloaded, quantity_loaded):
    if quantity_unloaded > 1 and quantity_loaded <= 1:
        return 'discharge', str(quantity_unloaded)
    elif quantity_unloaded <= 1 and quantity_loaded > 1:
        return 'load', str(quantity_loaded)
    elif quantity_unloaded > 1 and quantity_loaded > 1:
        # Both denotes that two moves will be added
        return 'both', 0
    else:
        # Both volume are under 1 : skip cargo
        return 'skip', -1.0


def fill_cargoes(info_from_detail_page, item):
    cargoes = list()
    product_description = info_from_detail_page['product']
    product_description = substitute_B_M_pattern.sub('B', product_description)
    # Delete improper '.' that are found in product description (ex: GASOLINA.)
    product_description = product_description.replace('.', '')
    # Description format should be either 'discharge product/load product' or 'product'
    slash_nb = product_description.count('/')
    try:
        quantity_unloaded, quantity_loaded = info_from_detail_page['quantity'].split('/')
        if slash_nb == 0:
            # Only one product description. Try to identify move from it
            if skip_cargo(product_description):
                return cargoes

            movement, product = identify_move(product_description)

            if movement == 'load':
                cargoes.append(build_cargo(cargoes, product, movement, quantity_loaded))
            elif movement == 'discharge':
                cargoes.append(build_cargo(cargoes, product, movement, quantity_unloaded))
            else:
                # Try to identify move from quantities
                movement, volume = identify_single_move_from_quantities(
                    float(quantity_unloaded), float(quantity_loaded)
                )

                # Now movement and volume are identified, fill cargoes
                if movement == 'both':
                    # Two movements identified : add both
                    cargoes.append(build_cargo(cargoes, product, 'discharge', quantity_unloaded))
                    cargoes.append(build_cargo(cargoes, product, 'load', quantity_loaded))
                elif movement in ['load', 'discharge']:
                    # Add cargo only if move identified
                    cargoes.append(build_cargo(cargoes, product, movement, volume))

        elif slash_nb == 1:
            # Potentially two different moves. For each part, identify move
            first_cargo, second_cargo = product_description.split('/')

            if not (skip_cargo(first_cargo)) and float(quantity_unloaded) > 1:
                movement, product = identify_move(first_cargo)
                cargoes.append(
                    build_cargo(
                        cargoes, product, movement if movement else 'discharge', quantity_unloaded
                    )
                )
            if not (skip_cargo(second_cargo)) and float(quantity_loaded) > 1:
                movement, product = identify_move(second_cargo)
                cargoes.append(
                    build_cargo(cargoes, product, movement if movement else 'load', quantity_loaded)
                )
        else:
            # Check to identify move from quantities
            # This case should not happen
            movement, volume = identify_single_move_from_quantities(
                float(quantity_unloaded), float(quantity_loaded)
            )
            # When movement not sure, skip it
            if movement in ['load', 'discharge']:
                cargoes.append(build_cargo(cargoes, product_description, movement, volume))
    except Exception:
        return cargoes

    return cargoes


def build_cargo(cargoes, product, movement, volume):
    return Cargo(
        product=product, movement=movement, volume=volume, commodity=None, volume_unit='tons'
    )


def secondary_table_parser(cache_info, table, info_from_detail_page, item):
    """
    Parse secondary table, or retrieve information from cache if exists.
    Fill item and info_from_detail_page with data, and update cache if needed.
    Note that argument are modified within function.
    Args:
        cache_info: dict containing information already retrieved from past request
        table: table retrieved from current request. If equal to None, no request made
        info_from_detail_page : dict containing info used to fill cargoes
        item : dict that will be yield
    """
    MMSI = None
    origin_harbor = None
    destination_harbor = None
    boat_type = None
    secondery_eta = None

    if info_from_detail_page['viaje_number'] in list(cache_info.keys()):
        current_viaje_info = cache_info[info_from_detail_page['viaje_number']]
        MMSI = current_viaje_info['MMSI']
        boat_type = current_viaje_info['boat_type']
        origin_harbor = current_viaje_info['origin_harbor']
        destination_harbor = current_viaje_info['destination_harbor']
        secondery_eta = current_viaje_info['secondery_eta']
    elif table:
        MMSI = try_apply(return_data_if_correct_field(table[6].extract(), 'MMSI'), int, str)
        boat_type = return_data_if_correct_field(table[15].extract(), 'TIPO_BUQUE')
        origin_harbor = return_data_if_correct_field(table[11].extract(), 'PUERTO_ORIGEN')
        destination_harbor = return_data_if_correct_field(table[12].extract(), 'PUERTO_DESTINO')
        if go_to_fish_pattern.search(origin_harbor) is not None:
            origin_harbor = None
        if go_to_fish_pattern.search(destination_harbor) is not None:
            destination_harbor = None
        secondery_eta = return_data_if_correct_field(table[18].extract(), 'FECHA_ETA')
        # Update cache info
        new_viaje_info = dict()
        new_viaje_info['MMSI'] = MMSI
        new_viaje_info['boat_type'] = boat_type
        new_viaje_info['origin_harbor'] = origin_harbor
        new_viaje_info['destination_harbor'] = destination_harbor
        new_viaje_info['secondery_eta'] = secondery_eta
        cache_info[info_from_detail_page['viaje_number']] = new_viaje_info

    # NOTE we don't yield foreign ETA due to a bug on the ETL
    # Precise some information and ensure correct eta : could be missing in table
    # if 'previous_zone' in list(item.keys()) and origin_harbor:
    #     item['previous_zone'] = origin_harbor

    if type(item) == EtaEvent:
        # Next line is commented as it caused a bug : when specified, 'next_zone
        # if destination_harbor:
        #     item['next_zone'] = destination_harbor
        # Next line enables to specify eta from detail page when not found in first scrapped page
        if item['eta'] is None and secondery_eta:
            item['eta'] = extract_utc_time(secondery_eta, ('p' in secondery_eta))
            item['matching_date'] = item['eta']

    # TODO : Add in Cargo origin and destination ?
    info_from_detail_page['origin_harbor'] = origin_harbor
    info_from_detail_page['destination_harbor'] = destination_harbor
    info_from_detail_page['boat_type'] = boat_type

    item['cargoes'] = fill_cargoes(info_from_detail_page, item)

    item['vessel'] = VesselIdentification(
        name=info_from_detail_page['vessel_name'],
        mmsi=MMSI,
        length=info_from_detail_page['vessel_length'],
        flag=info_from_detail_page['vessel_flag'],
    )
