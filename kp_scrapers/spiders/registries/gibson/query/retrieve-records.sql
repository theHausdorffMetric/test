SELECT DISTINCT
    /* vessel attributes */
    EAGKplerVesselMain.vesselname,
    EAGKplerVesselMain.imonumber,
    EAGKplerVesselMain.callsign,
    EAGKplerVesselMain.flagcode,
    EAGKplerVesselMain.vesseltypecode,
    EAGKplerVesselType.typename,
    EAGKplerVesselMain.subtypecode,
    EAGKplerSubtype.subtypename,
    EAGKplerTradingCategory.tradingcategoryname,
    EAGKplerVesselMain.tradingstatuscode,
    EAGKplerTradingStatus.tradingStatusName,
    EAGKplerTradingStatus.tradingcategorycode,
    EAGKplerVesselMain.orderyear,
    EAGKplerVesselMain.ordermonth,
    EAGKplerVesselMain.launchyear,
    EAGKplerVesselMain.launchmonth,
    EAGKplerVesselMain.builtyear,
    EAGKplerVesselMain.builtmonth,
    EAGKplerVesselMain.dwt,
    EAGKplerVesselMain.draft,
    EAGKplerVesselMain.nt,
    EAGKplerVesselMain.gt,
    EAGKplerVesselMain.loa,
    EAGKplerVesselMain.beammoulded,
    EAGKplerVesselMain.liquidcubic98pcnt,
    EAGKplerVesselMain.graincubic,
    EAGKplerVesselMain.balecubic,
    EAGKplerVesselMain.orecubic,
    EAGKplerVesselMain.ldt,
    EAGKplerVesselMain.lbp,
    EAGKplerVesselMain.depth,
    EAGKplerVesselMain.ladenspeed,
    EAGKplerVesselMain.ballastspeed,
    EAGKplerVesselMain.ladenconsumption,
    EAGKplerVesselMain.ballastconsumption,
    EAGKplerVesselMain.draft,
    EAGKplerVesselMain.graincubic,
    EAGKplerVesselMain.lastdrydock,
    EAGKplerVesselMain.lastspecialsurvey,
    EAGKplerVesselMain.displacement,
    EAGKplerVesselMain.keellaidyear,
    EAGKplerVesselMain.keellaidmonth,
    EAGKplerVesselMain.keellaidday,
    EAGKplerVesselMain.orderday,
    EAGKplerVesselMain.launchday,
    EAGKplerVesselMain.builtday,
    EAGKplerVesselMain.deadyear,
    EAGKplerVesselMain.deadmonth,
    EAGKplerVesselMain.deadday,
    EAGKplerVesselMain.tpcmi,
    EAGKplerVesselMain.ldt,
    EAGKplerVesselMain.ntsuez,
    EAGKplerVesselMain.ntpanama,
    EAGKplerVesselMain.scrubberfitted,
    EAGKplerVesselMain.scrubberready,
    EAGKplerVesselMain.scrubberplanned,
    EAGKplerScrubberType.scrubbertypename,
    EAGKplerVesselMain.scrubberdate,
    /* owner attributes */
    Owner.shortname Owner,
    EAGKplerVesselMain.commercialownereffdate,
    /* commercial manager attributes */
    Manager.shortname Manager,
    EAGKplerVesselMain.primaryoperatoreffdate,
    /* current charterer attributes */
    Charterer.shortname Charterer,
    EAGKplerVesselMain.effectivecontroleffdate,
    EAGKplerVesselMain.effectivecontrolexpiry
FROM EAGKplerVesselMain
LEFT JOIN EAGKplerScrubberType ON EAGKplerScrubberType.code = EAGKplerVesselMain.ScrubberTypeCode
LEFT JOIN EAGKplerVesselType ON EAGKplerVesselType.code = EAGKplerVesselMain.vesseltypecode
LEFT JOIN EAGKplerSubtype ON EAGKplerSubtype.code = EAGKplerVesselMain.subtypecode
LEFT JOIN EAGKplerTradingCategory ON EAGKplerTradingCategory.code = EAGKplerVesselMain.tradingcategorycode
LEFT JOIN EAGKplerTradingStatus ON EAGKplerTradingStatus.code = EAGKplerVesselMain.tradingstatuscode
LEFT JOIN (
    SELECT EAGKplerCompany.*, EAGKplerCompanytype.companytype
    FROM EAGKplerCompany
    LEFT JOIN EAGKplerCompany parent ON parent.id = EAGKplerCompany.groupcompanyid
    LEFT JOIN EAGKplerCompanytype ON EAGKplerCompanytype.id = EAGKplerCompany.companytypeid
    LEFT JOIN EAGKplerCompanytype parenttype  ON parenttype.id = parent.companytypeid
) Owner ON Owner.id = EAGKplerVesselMain.commercialownerid
LEFT JOIN (
    SELECT EAGKplerCompany.*, EAGKplerCompanytype.companytype
    FROM EAGKplerCompany
    LEFT JOIN EAGKplerCompany parent ON parent.id = EAGKplerCompany.groupcompanyid
    LEFT JOIN EAGKplerCompanytype ON EAGKplerCompanytype.id = EAGKplerCompany.companytypeid
    LEFT JOIN EAGKplerCompanytype parenttype  ON parenttype.id = parent.companytypeid
) Manager ON Manager.id = EAGKplerVesselMain.primaryoperatorid
LEFT JOIN (
    SELECT EAGKplerCompany.*, EAGKplerCompanytype.companytype
    FROM EAGKplerCompany
    LEFT JOIN EAGKplerCompany parent ON parent.id = EAGKplerCompany.groupcompanyid
    LEFT JOIN EAGKplerCompanytype ON EAGKplerCompanytype.id = EAGKplerCompany.companytypeid
    LEFT JOIN EAGKplerCompanytype parenttype  ON parenttype.id = parent.companytypeid
) Charterer ON Charterer.id = EAGKplerVesselMain.effectivecontrolid
