import ast
import logging
import re


logger = logging.getLogger(__name__)


_JODI_PRIMARY = {
    'sWD_ReportId': '{report_id}',
    'sWD_TableId': '{table_id}',
    'sWD_ReportView': '<ReportView><RowDims><Dim name="Country"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="06375c8d-16d8-4155-9b20-bc63cca79223"><Group id="06375c8d-16d8-4155-9b20-bc63cca79223" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim></RowDims><ColDims><Dim name="Time"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="934dda2e-31dc-446d-81a5-1b667b019300"><Group id="934dda2e-31dc-446d-81a5-1b667b019300" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim></ColDims><OtherDims><Dim name="Unit"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="dc6de0b1-aa75-4961-859d-9811f1259e97"><Group id="dc6de0b1-aa75-4961-859d-9811f1259e97" type="Selection"><Definition><All/></Definition><ActiveItem pos="{unit}" /></Group></Groups></Dim><Dim name="Product"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="7c980a7c-abf2-4218-8ee5-54e2995b36e5"><Group id="7c980a7c-abf2-4218-8ee5-54e2995b36e5" type="Selection"><Definition><All/></Definition><ActiveItem pos="{product}" /></Group></Groups></Dim><Dim name="BALANCE"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="4e69f9ea-d72c-4e7e-9f02-1e31ed9d394b"><Group id="4e69f9ea-d72c-4e7e-9f02-1e31ed9d394b" type="Selection"><Definition><All/></Definition><ActiveItem pos="{balance}" /></Group></Groups></Dim></OtherDims><Chart type="" rows="-1" cols="-1" labels="False" /></ReportView>',  # noqa
    'sWD_MaxDim': '5',
    'sWD_ActiveMember': '-1',
    'sWD_SelectedItemsCount': '5,4,10,114,206',
    'sWD_CurrentDimSelectedCount': '-1',
    'sWD_CurrentDimHasTotal': 'False',
    'WD_GroupsModified': 'False',
    'oWD_DimActiveItemId': '{unit},{product},{balance},0,0',
    'WD_SearchLabel': '',
    'WD_SearchLang': '',
    'WD_ExpItems_Dim0': '<All/>',
    'WD_ItemFirstRow_Dim0': '0',
    'WD_ItemVPos_Dim0': '0',
    'WD_ExpItems_Dim1': '<All/>',
    'WD_ItemFirstRow_Dim1': '0',
    'WD_ItemVPos_Dim1': '0',
    'WD_ExpItems_Dim2': '<All/>',
    'WD_ItemFirstRow_Dim2': '0',
    'WD_ItemVPos_Dim2': '0',
    'WD_ExpItems_Dim3': '<All/>',
    'WD_ItemFirstRow_Dim3': '0',
    'WD_ItemVPos_Dim3': '0',
    'WD_ExpItems_Dim4': '<All/>',
    'WD_ItemFirstRow_Dim4': '0',
    'WD_ItemVPos_Dim4': '0',
    'WD_ActiveGroup': '',
    'sWD_RowsItemsCount': '206',
    'sWD_ColsItemsCount': '114',
    'sWD_Cols': '3',
    'sWD_Rows': '4',
    'sWD_Others': '0,1,2',
    'sWD_LastViewer': 'T158649',
    'sWD_FirstRow': '0',
    'sWD_ChunkFirstRow': '',
    'sWD_FirstDisplayRow': '0',
    'sWD_FirstCol': '0',
    'sWD_ChunkFirstCol': '',
    'sWD_FirstDisplayCol': '0',
    'sWD_FirstItem': '0',
    'IF_ReportType': '1',
    'sWD_PosX': '0',
    'sWD_PosY': '0',
    'sWD_TotalPercent': '0',
    'sWD_VPos': '0',
    'WD_Printable': '0',
    'sWD_SourceInfo': '',
    'sWD_MaxProfileItems': '100',
    'sWD_DataFirstRow': '1',
    'sWD_DataFirstCol': '1',
    'sWD_RowsPerPage': '20',
    'sWD_ColumnsPerPage': '20',
    'sWD_Permit': '-1',
    'sWD_ReportFolder': '38670',
    'sWD_Ev': '0',
    'sWD_Referrer': '',
    'sWD_PrintOrientation': '1',
    'sWD_PrintPaperSize': '4',
    'sWD_PrintFontSize': '1',
    'sWD_PrintColour': '1',
    'sWD_PrintMargins': '0.50,0.50,0.50,0.50',
    'sRF_SortField': '',
    'sRF_SortAscending': 'False',
    'sRF_ActivePath': 'P,38670',
    'sRF_Mode': '',
    'sRF_User': '',
    'sRF_Expanded': '',
    'sRF_Task': '0',
    'sRF_PosX': '0',
    'sRF_PosY': '0',
    'sRF_ViewTop': '0',
    'sRF_ShowFolders': '1',
    'sRF_PanePosition': '200',
    'sRF_SearchStringBuf': '',
    'sRF_SearchRangeBuf': '',
    'sRF_SearchTypeBuf': '',
    'sRF_SearchExactWordBuf': 'True',
    'sRF_SearchProperties': '',
    'sRF_ScopedSearching': 'False',
    'sRF_InManageReportsMode': 'False',
    'sRF_SearchFromMap': 'False',
    'sRF_SearchPerform': 'False',
    'sRF_SearchReportIDs': '',
    'sRF_SearchFolder': '0',
    'sRF_PreviousTask': '',
    'sRF_ScrollPosition': '',
    'sRF_SameTitle': 'True',
    'sRF_IncludePA': 'False',
    'sCS_referer': '',
    'sCS_ChosenLang': 'en',
    'sCS_AppPath': '',
    'sCS_SpawnWindow': 'True',
    'CS_InHelp': 'False',
    'CS_SaveMode': 'True',
    'CS_langSwitch': '',
    'CS_TargetPage': '',
    'CS_demoIndex': '0',
    'CS_HelpPage': '',
    'CS_FramesInHelp': 'True',
    'CS_Printable': 'False',
    'CS_TableauHeight': '202',
    'CS_TableauWidth': '1440',
    'CS_ActiveXEnabled': 'false',
    'CS_ReportTitle': 'Joint Organisations Data Initiative – Primary (all data)',
    'CS_NextPage': '/TableViewer/tableView.aspx',
    'sCS_DownloadLimit': '15000',
    'CS_IVTReportIsOpened': 'True',
    'CS_bIVTReportHasMapTab': 'False',
    'LG_targetpage': '',
    'LG_reprompt': '',
    'PR_ActiveProfileList': '',
    'PR_NextPage': '',
    'PR_TableLang': 'eng',
    'PR_TableDefaultLang': 'eng',
    'PR_Mode': '0',
}

_JODI_SECONDARY = {
    'sWD_ReportId': '{report_id}',
    'sWD_TableId': '{table_id}',
    'sWD_ReportView': '<ReportView><RowDims><Dim name="Country"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="5422fab3-5614-4260-b3c6-0913bebf75a8"><Group id="5422fab3-5614-4260-b3c6-0913bebf75a8" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim></RowDims><ColDims><Dim name="Time"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="1482c2a0-e605-4f36-b3ef-c8df7cfd3ec2"><Group id="1482c2a0-e605-4f36-b3ef-c8df7cfd3ec2" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim></ColDims><OtherDims><Dim name="Unit"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="160f5d81-7eba-4e86-a97f-78f9f781cbc2"><Group id="160f5d81-7eba-4e86-a97f-78f9f781cbc2" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim><Dim name="Product"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="22444b93-ecf1-4c44-947b-1e694c683b7a"><Group id="22444b93-ecf1-4c44-947b-1e694c683b7a" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim><Dim name="BALANCE"><DimLangs><DimLang lang="eng"><DefaultProperty propertyIndex="1" /></DimLang></DimLangs><Groups activeGroup="d9b1e99b-61ee-4b14-83ec-f98bd8c07b5f"><Group id="d9b1e99b-61ee-4b14-83ec-f98bd8c07b5f" type="Selection"><Definition><All/></Definition><ActiveItem pos="0" /></Group></Groups></Dim></OtherDims><Chart type="" rows="-1" cols="-1" labels="False" /></ReportView>',  # noqa
    'sWD_MaxDim': '5',
    'sWD_ActiveMember': '-1',
    'sWD_SelectedItemsCount': '5,9,10,114,207',
    'sWD_CurrentDimSelectedCount': '-1',
    'sWD_CurrentDimHasTotal': 'False',
    'WD_GroupsModified': 'False',
    'oWD_DimActiveItemId': '{unit},{product},{balance},0,0',
    'WD_SearchLabel': '',
    'WD_SearchLang': '',
    'WD_ExpItems_Dim0': '<All/>',
    'WD_ItemFirstRow_Dim0': '0',
    'WD_ItemVPos_Dim0': '0',
    'WD_ExpItems_Dim1': '<All/>',
    'WD_ItemFirstRow_Dim1': '0',
    'WD_ItemVPos_Dim1': '0',
    'WD_ExpItems_Dim2': '<All/>',
    'WD_ItemFirstRow_Dim2': '0',
    'WD_ItemVPos_Dim2': '0',
    'WD_ExpItems_Dim3': '<All/>',
    'WD_ItemFirstRow_Dim3': '0',
    'WD_ItemVPos_Dim3': '0',
    'WD_ExpItems_Dim4': '<All/>',
    'WD_ItemFirstRow_Dim4': '0',
    'WD_ItemVPos_Dim4': '0',
    'WD_ActiveGroup': '',
    'sWD_RowsItemsCount': '206',
    'sWD_ColsItemsCount': '114',
    'sWD_Cols': '3',
    'sWD_Rows': '4',
    'sWD_Others': '0,1,2',
    'sWD_LastViewer': 'T161250',
    'sWD_FirstRow': '0',
    'sWD_ChunkFirstRow': '',
    'sWD_FirstDisplayRow': '0',
    'sWD_FirstCol': '0',
    'sWD_ChunkFirstCol': '',
    'sWD_FirstDisplayCol': '0',
    'sWD_FirstItem': '0',
    'IF_ReportType': '1',
    'sWD_PosX': '0',
    'sWD_PosY': '0',
    'sWD_TotalPercent': '0',
    'sWD_VPos': '0',
    'WD_Printable': '0',
    'sWD_SourceInfo': '',
    'sWD_MaxProfileItems': '100',
    'sWD_DataFirstRow': '1',
    'sWD_DataFirstCol': '1',
    'sWD_RowsPerPage': '20',
    'sWD_ColumnsPerPage': '20',
    'sWD_Permit': '-1',
    'sWD_ReportFolder': '38670',
    'sWD_Ev': '0',
    'sWD_Referrer': '',
    'sWD_PrintOrientation': '1',
    'sWD_PrintPaperSize': '4',
    'sWD_PrintFontSize': '1',
    'sWD_PrintColour': '1',
    'sWD_PrintMargins': '0.50,0.50,0.50,0.50',
    'sRF_SortField': '',
    'sRF_SortAscending': 'False',
    'sRF_ActivePath': 'P,38670',
    'sRF_Mode': '',
    'sRF_User': '',
    'sRF_Expanded': '',
    'sRF_Task': '0',
    'sRF_PosX': '0',
    'sRF_PosY': '0',
    'sRF_ViewTop': '0',
    'sRF_ShowFolders': '1',
    'sRF_PanePosition': '200',
    'sRF_SearchStringBuf': '',
    'sRF_SearchRangeBuf': '',
    'sRF_SearchTypeBuf': '',
    'sRF_SearchExactWordBuf': 'True',
    'sRF_SearchProperties': '',
    'sRF_ScopedSearching': 'False',
    'sRF_InManageReportsMode': 'False',
    'sRF_SearchFromMap': 'False',
    'sRF_SearchPerform': 'False',
    'sRF_SearchReportIDs': '',
    'sRF_SearchFolder': '0',
    'sRF_PreviousTask': '',
    'sRF_ScrollPosition': '',
    'sRF_SameTitle': 'True',
    'sRF_IncludePA': 'False',
    'sCS_referer': '',
    'sCS_ChosenLang': 'en',
    'sCS_AppPath': '',
    'sCS_SpawnWindow': 'True',
    'CS_InHelp': 'False',
    'CS_SaveMode': 'True',
    'CS_langSwitch': '',
    'CS_TargetPage': '',
    'CS_demoIndex': '0',
    'CS_HelpPage': '',
    'CS_FramesInHelp': 'True',
    'CS_Printable': 'False',
    'CS_TableauHeight': '202',
    'CS_TableauWidth': '1440',
    'CS_ActiveXEnabled': 'false',
    'CS_ReportTitle': 'Joint Organisations Data Initiative – Secondary (all data)',
    'CS_NextPage': '/TableViewer/tableView.aspx',
    'sCS_DownloadLimit': '15000',
    'CS_IVTReportIsOpened': 'True',
    'CS_bIVTReportHasMapTab': 'False',
    'LG_targetpage': '',
    'LG_reprompt': '',
    'PR_ActiveProfileList': '',
    'PR_NextPage': '',
    'PR_TableLang': 'eng',
    'PR_TableDefaultLang': 'eng',
    'PR_Mode': '0',
}


JODI_FORM = {'0': _JODI_PRIMARY, '1': _JODI_SECONDARY}


def get_form(body, report_type, unit, product, balance):
    """Get report id and table id from response body.

    http://www.jodidb.org/ReportFolders/reportFolders.aspx?sCS_referer=&sCS_ChosenLang=en

    It listed four reports:
        - primary all data
        - secondary all data
        - primary last 15 months
        - secondary last 15 months

    The params pair we get is for constructing form data to retrieve reports, we mainly deal with
    the first two reports, using type argument to specify.


    Args:
        body (str):
        report_type (str): 0 for primary, 1 for secondary
        unit (str):
        product (str):
        balance (str):

    Returns:
        Dict[str, str]:

    """
    gan_id_search = re.search(r'(?:ganId\s*=\s*)([\[\]\d,]+)', body)
    gan_source_id_search = re.search(r'(?:ganSourceId\s*=\s*)([\[\]\d,]+)', body)

    if not gan_id_search or not gan_source_id_search:
        logger.error('JODI resource may have changed, could not parse parameters')
        return None

    _gan_ids = ast.literal_eval(gan_id_search.group(1))
    _gan_source_ids = ast.literal_eval(gan_source_id_search.group(1))
    report_id = _gan_ids[int(report_type)]
    table_id = _gan_source_ids[int(report_type)]

    form = JODI_FORM[report_type]
    form.update(
        sWD_ReportId=form['sWD_ReportId'].format(report_id=report_id),
        sWD_TableId=form['sWD_TableId'].format(table_id=table_id),
        sWD_ReportView=form['sWD_ReportView'].format(unit=unit, product=product, balance=balance),
        oWD_DimActiveItemId=form['oWD_DimActiveItemId'].format(
            unit=unit, product=product, balance=balance
        ),
    )

    return form
