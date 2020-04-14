"""Module for maintining a repository of supported units.

These units are meant to be kept in sync with those specified in `ct-pipeline`.
For more details: https://github.com/Kpler/ct-pipeline/blob/master/data/units.txt

This units definition module keeps a trimmed set of ETL units exposed for other modules to use,
to reduce confusion and to keep consistent naming styles.

"""
from kp_scrapers.models.enum import Enum


Unit = Enum(
    # Dimension
    meter='meter',
    kilometer='kilometer',
    # Mass
    gram='gram',
    kilogram='kilogram',
    tons='tons',
    kilotons='kilotons',
    megatons='megatons',
    # Volume
    liter='liter',
    kiloliter='kiloliter',
    cubic_meter='cubic_meter',  # `cm` is not used to avoid namespace conflict with `centimeter`
    cubic_feet='cubic_feet',
    barrel='barrel',
    kilobarrel='kilobarrel',
    megabarrel='megabarrel',
    # Time
    second='second',
    minute='minute',
    hour='hour',
    day='day',
    week='week',
    month='month',
    year='year',
    # Power
    watt='W',
    # Energy
    joule='joule',
    Btu='Btu',  # british thermal unit
    therm='therm',  # equal to 100000 Btu
    watt_hour='Wh',
    kilowatt_hour='kWh',
    megawatt_hour='MWh',
    # Force
    newton='N',
    # Pressure
    pascal='Pa',
    # Temperature
    celsius='C',
    kelvin='K',
    # Mass Flow Rate
    tons_per_annum='tpa',
    # Speed
    knots='knots',
)


Currency = Enum(
    AED='AED',  # United Arab Emirates Dirham
    ARS='ARS',  # Argentine Peso
    AUD='AUD',  # Australian Dollar
    AZN='AZN',  # Azerbaijani Manat
    BGN='BGN',  # Bulgarian Lev
    BHD='BHD',  # Bahraini Dinar
    BND='BND',  # Brunei Dollar
    BRL='BRL',  # Brazilian Real
    CAD='CAD',  # Canadian Dollar
    CHF='CHF',  # Swiss Franc
    CLP='CLP',  # Chilean Peso
    CNY='CNY',  # Chinese Yuan
    CZK='CZK',  # Czech Koruna
    DKK='DKK',  # Danish Krone
    EGP='EGP',  # Egyptian Pound
    EUR='EUR',  # European Euro
    FJD='FJD',  # Fiji Dollar
    GBP='GBP',  # British Sterling Pound
    HKD='HKD',  # Hong Kong Dollar
    HUF='HUF',  # Hungarian Forint
    IDR='IDR',  # Indonesian Rupiah
    ILS='ILS',  # Israeli New Shekel
    INR='INR',  # Indian Rupee
    JPY='JPY',  # Japanese Yen
    KRW='KRW',  # South Korean Won
    KWD='KWD',  # Kuwaiti Dinar
    LKR='LKR',  # Sri Lankan Rupee
    MAD='MAD',  # Moroccan Dirham
    MGA='MGA',  # Malagasy Ariary
    MXN='MXN',  # Mexican Peso
    MYR='MYR',  # Malaysian Ringgit
    NOK='NOK',  # Norwegian Krone
    NZD='NZD',  # New Zealand Dollar
    OMR='OMR',  # Omani Rial
    PEN='PEN',  # Peruvian Sol
    PGK='PGK',  # Papua New Guinean Kina
    PHP='PHP',  # Philippine Peso
    PKR='PKR',  # Pakistani Rupee
    PLN='PLN',  # Polish Złoty
    QAR='QAR',  # Qatari Rial
    RUB='RUB',  # Russian Ruble
    SAR='SAR',  # Saudi Riyal
    SBD='SBD',  # Solomon Islands Dollar
    SCR='SCR',  # Seychelles Rupee
    SEK='SEK',  # Swedish Krona/Kronor
    SGD='SGD',  # Singapore Dollar
    THB='THB',  # Thai Baht
    TOP='TOP',  # Tongan Paʻanga
    TWD='TWD',  # New Taiwan Dollar
    TZS='TZS',  # Tanzanian Shilling
    USD='USD',  # United States Dollar
    VEF='VEF',  # Venezuelan Bolívar
    VND='VND',  # Vietnamese Dồng
    VUV='VUV',  # Vanuatu Vatu
    WST='WST',  # Samoan Tala
    XOF='XOF',  # CFA Franc BCEAO
    ZAR='ZAR',  # South African Rand
)
