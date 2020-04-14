from sqlalchemy import Column, DateTime, Integer, String

from kp_scrapers.redshift_orm.declarative_base import ABase


class CargoMovementStats(ABase):
    __table_args__ = {'schema': 'production'}
    __tablename__ = 'cargo_movement_stats'

    sh_spider_name = Column(String, nullable=False)
    # NOTE: redshift sqlalchemy requires a primary key and id_seq, to
    # by pass this, primary_key was assigned to sh_job_id with auto
    # increment set to false
    sh_job_id = Column(String, primary_key=True, autoincrement=False)
    sh_job_time = Column(DateTime, nullable=False)
    sh_job_finish_time = Column(DateTime, nullable=False)
    datatype = Column(String, nullable=False)
    total_item_count = Column(Integer, default=0)
    date_eta_count = Column(Integer, default=0)
    date_arrival_count = Column(Integer, default=0)
    date_berth_count = Column(Integer, default=0)
    date_departure_count = Column(Integer, default=0)
    vessel_name_count = Column(Integer, default=0)
    vessel_imo_count = Column(Integer, default=0)
    vessel_beam_count = Column(Integer, default=0)
    vessel_buildyear_count = Column(Integer, default=0)
    vessel_length_count = Column(Integer, default=0)
    vessel_mmsi_count = Column(Integer, default=0)
    vessel_callsign_count = Column(Integer, default=0)
    vessel_deadweight_count = Column(Integer, default=0)
    vessel_flagcode_count = Column(Integer, default=0)
    vessel_flagname_count = Column(Integer, default=0)
    vessel_grosston_count = Column(Integer, default=0)
    vessel_vesseltype_count = Column(Integer, default=0)
    cargo_product_count = Column(Integer, default=0)
    cargo_movement_count = Column(Integer, default=0)
    cargo_volume_count = Column(Integer, default=0)
    cargo_volumeunit_count = Column(Integer, default=0)
    cargo_buyername_count = Column(Integer, default=0)
    cargo_sellername_count = Column(Integer, default=0)
    cargoes_product_count = Column(Integer, default=0)
    cargoes_movement_count = Column(Integer, default=0)
    cargoes_volume_count = Column(Integer, default=0)
    cargoes_volumeunit_count = Column(Integer, default=0)
    cargoes_buyername_count = Column(Integer, default=0)
    cargoes_sellername_count = Column(Integer, default=0)
    location_berth_count = Column(Integer, default=0)
    location_portname_count = Column(Integer, default=0)
    location_installation_count = Column(Integer, default=0)
    location_nextzone_count = Column(Integer, default=0)
    error_msg_count = Column(Integer, default=0)
    warning_msg_count = Column(Integer, default=0)
    exception_msg_count = Column(Integer, default=0)


class SpotCharterStats(ABase):
    __table_args__ = {'schema': 'production'}
    __tablename__ = 'spot_charter_stats'

    sh_spider_name = Column(String, nullable=False)
    # NOTE: redshift sqlalchemy requires a primary key and id_seq, to
    # by pass this, primary_key was assigned to sh_job_id with auto
    # increment set to false
    sh_job_id = Column(String, primary_key=True, autoincrement=False)
    sh_job_time = Column(DateTime, nullable=False)
    sh_job_finish_time = Column(DateTime, nullable=False)
    datatype = Column(String, nullable=False)
    total_item_count = Column(Integer, default=0)
    location_arrivalzone_count = Column(Integer, default=0)
    location_departurezone_count = Column(Integer, default=0)
    date_laycanstart_count = Column(Integer, default=0)
    date_laycanend_count = Column(Integer, default=0)
    att_charterer_count = Column(Integer, default=0)
    att_status_count = Column(Integer, default=0)
    vessel_name_count = Column(Integer, default=0)
    vessel_imo_count = Column(Integer, default=0)
    vessel_beam_count = Column(Integer, default=0)
    vessel_buildyear_count = Column(Integer, default=0)
    vessel_length_count = Column(Integer, default=0)
    vessel_mmsi_count = Column(Integer, default=0)
    vessel_callsign_count = Column(Integer, default=0)
    vessel_deadweight_count = Column(Integer, default=0)
    vessel_flagcode_count = Column(Integer, default=0)
    vessel_flagname_count = Column(Integer, default=0)
    vessel_grosston_count = Column(Integer, default=0)
    vessel_vesseltype_count = Column(Integer, default=0)
    cargo_product_count = Column(Integer, default=0)
    cargo_movement_count = Column(Integer, default=0)
    cargo_volume_count = Column(Integer, default=0)
    cargo_volumeunit_count = Column(Integer, default=0)
    cargo_buyername_count = Column(Integer, default=0)
    cargo_sellername_count = Column(Integer, default=0)
    error_msg_count = Column(Integer, default=0)
    warning_msg_count = Column(Integer, default=0)
    exception_msg_count = Column(Integer, default=0)
