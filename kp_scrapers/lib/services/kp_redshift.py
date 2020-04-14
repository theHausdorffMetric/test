import datetime as dt
import logging
from typing import Dict

from kp_scrapers.lib.date import system_tz_offset
from kp_scrapers.lib.db_connection import db_conn_orm
from kp_scrapers.lib.services.shub import global_settings as Settings
from kp_scrapers.lib.utils import ignore_key, map_keys
from kp_scrapers.redshift_orm.loader_orm import CargoMovementStats, SpotCharterStats


logger = logging.getLogger(__name__)


def upsert_metrics(stats: Dict):
    with db_conn_orm(Settings()['MONITORING_DB']) as sesh:
        if 'spotcharter' in stats.get('datatype'):
            item = map_keys(stats.get('spider_attribute_stats'), sc_mapping())
            citem = map_keys(stats, common_mapping())
            item.update(
                citem, datatype='spotcharter',
            )
            spider_stats = SpotCharterStats(**item)
        elif 'portcall' in stats.get('datatype'):
            item = map_keys(stats.get('spider_attribute_stats'), cml_mapping())
            citem = map_keys(stats, common_mapping())
            item.update(
                citem, datatype='portcall',
            )
            spider_stats = CargoMovementStats(**item)
        elif 'cargo_movement' in stats.get('datatype'):
            item = map_keys(stats.get('spider_attribute_stats'), cml_mapping())
            citem = map_keys(stats, common_mapping())
            item.update(
                citem, datatype='cargo_movement',
            )
            spider_stats = CargoMovementStats(**item)
        else:
            logger.info('redshift monitoring not supported')
            return

        sesh.add(spider_stats)
        sesh.commit()


def common_mapping():
    return {
        'warning_count': ('warning_msg_count', None),
        'error_count': ('error_msg_count', None),
        'exception_count': ('exception_msg_count', None),
        'start_time': (
            'sh_job_time',
            lambda x: (x - dt.timedelta(hours=system_tz_offset())).isoformat(),
        ),
        'finish_time': (
            'sh_job_finish_time',
            lambda x: (x - dt.timedelta(hours=system_tz_offset())).isoformat(),
        ),
        'total_items': ('total_item_count', None),
        'spider_attribute_stats': ignore_key('handled later'),
        'sh_spider_name': ('sh_spider_name', None),
        'sh_job_id': ('sh_job_id', None),
        'datatype': ('datatype', None),
    }


def cml_mapping():
    return {
        'eta': ('date_eta_count', None),
        'arrival': ('date_arrival_count', None),
        'berthed': ('date_berth_count', None),
        'departure': ('date_departure_count', None),
        'port_name': ('location_portname_count', None),
        'installation': ('location_installation_count', None),
        'berth': ('location_berth_count', None),
        'vessel.name': ('vessel_name_count', None),
        'vessel.imo': ('vessel_imo_count', None),
        'vessel.beam': ('vessel_beam_count', None),
        'vessel.build_year': ('vessel_buildyear_count', None),
        'vessel.length': ('vessel_length_count', None),
        'vessel.mmsi': ('vessel_mmsi_count', None),
        'vessel.callsign': ('vessel_callsign_count', None),
        'vessel.dead_weight': ('vessel_deadweight_count', None),
        'vessel.flag_code': ('vessel_flagcode_count', None),
        'vessel.flag_name': ('vessel_flagname_count', None),
        'vessel.gross_tonnage': ('vessel_grosston_count', None),
        'vessel.vessel_type': ('vessel_vesseltype_count', None),
        'cargo.movement': ('cargo_movement_count', None),
        'cargo.product': ('cargo_product_count', None),
        'cargo.volume': ('cargo_volume_count', None),
        'cargo.volume_unit': ('cargo_volumeunit_count', None),
        'cargo.buyer.name': ('cargo_buyername_count', None),
        'cargo.seller.name': ('cargo_sellername_count', None),
        'cargoes.movement': ('cargoes_movement_count', None),
        'cargoes.product': ('cargoes_product_count', None),
        'cargoes.volume': ('cargoes_volume_count', None),
        'cargoes.volume_unit': ('cargoes_volumeunit_count', None),
        'cargoes.buyer.name': ('cargoes_buyername_count', None),
        'cargoes.seller.name': ('cargoes_sellername_count', None),
    }


def sc_mapping():
    return {
        'arrival_zone.': ('location_arrivalzone_count', None),
        'departure_zone': ('location_departurezone_count', None),
        'charterer': ('att_charterer_count', None),
        'status': ('att_status_count', None),
        'lay_can_start': ('date_laycanstart_count', None),
        'lay_can_end': ('date_laycanend_count', None),
        'vessel.name': ('vessel_name_count', None),
        'vessel.imo': ('vessel_imo_count', None),
        'vessel.beam': ('vessel_beam_count', None),
        'vessel.build_year': ('vessel_buildyear_count', None),
        'vessel.length': ('vessel_length_count', None),
        'vessel.mmsi': ('vessel_mmsi_count', None),
        'vessel.callsign': ('vessel_callsign_count', None),
        'vessel.dead_weight': ('vessel_deadweight_count', None),
        'vessel.flag_code': ('vessel_flagcode_count', None),
        'vessel.flag_name': ('vessel_flagname_count', None),
        'vessel.gross_tonnage': ('vessel_grosston_count', None),
        'vessel.vessel_type': ('vessel_vesseltype_count', None),
        'cargo.movement': ('cargo_movement_count', None),
        'cargo.product': ('cargo_product_count', None),
        'cargo.volume': ('cargo_volume_count', None),
        'cargo.volume_unit': ('cargo_volumeunit_count', None),
        'cargo.buyer.name': ('cargo_buyername_count', None),
        'cargo.seller.name': ('cargo_sellername_count', None),
    }
