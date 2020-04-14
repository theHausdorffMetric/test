# -*- coding: utf-8; -*-

from __future__ import absolute_import, unicode_literals
import json
import os

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from httplib2 import Http
from oauth2client import client, file, tools
from oauth2client.service_account import ServiceAccountCredentials

from kp_scrapers.lib.services.shub import global_settings as Settings, validate_settings
from kp_scrapers.lib.utils import retry
from kp_scrapers.lib.xls import Workbook


API_VERSION = 'v3'
PDF_DOWNLOAD_URL_TPL = 'https://www.googleapis.com/drive/v3/files/{id_}?alt=media'
# good enough approximation to clear or write the content we need
ALL_CELLS = 'A:ZZ'


class GDriveFileType(object):
    FILE = 'GDriveFile'
    FOLDER = 'GDriveFolder'


class GDriveMimeTypes(object):
    PDF = ['application/pdf']
    SPREADSHEETS = [
        'application/vnd.ms-excel',  # xls
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # xlsx
        'application/vnd.google-apps.spreadsheet',  # google spreadsheet
        'application/vnd.oasis.opendocument.spreadsheet',  # ods
    ]


def build_query(
    base_query=None,
    type_=None,
    mimes=None,
    name=None,
    parent_id=None,
    included_tags=None,
    excluded_tags=None,
):
    """Builds a query string from a list of queries and additional filters like
    the name or the type of the file.
    The resulting query can be used by GDriveService to retrieve files following a filter.

    Official documentation:
        https://developers.google.com/drive/v2/web/search-parameters

    Example:
        Building a query to retrieve all children (files and folders) of a given folder id
        >>> original_query = build_query(parent_id='a_unique_id')

        Refining the query to only keep spreadsheets with a `to_be_processed` tag
        >>> tagged_xls_query = build_query(base_query=original_query,
        ...                                mimes=GDriveMimeTypes.SPREADSHEETS,
        ...                                included_tags=['to_be_processed'])

        Refining the original query to only keep folders
        >>> folders_query = build_query(base_query=original_query, type_=GDriveFileType.FOLDER)

    Args:
        base_query (str | unicode | None)
        type_ (str | unicode | None): A GDriveFileType to use as filter
        mimes (list[str | unicode] | None): A list of mime types (GDriveMimeTypes) to use as filter
        name (str | unicode | None)
        parent_id (str | unicode | None): id of the parent folder
        included_tags (list[str | unicode] | None): tags that must be on the files
        excluded_tags (list[str | unicode] | None): tags that must not be on the files

    """
    queries = [base_query]
    base_properties_query_tpl = 'properties has {{key="{}" and value="True"}}'

    for tag in included_tags or []:
        query = base_properties_query_tpl.format(tag)
        queries.append(query)

    for tag in excluded_tags or []:
        query = 'not ' + base_properties_query_tpl.format(tag)
        queries.append(query)

    if type_:
        files_only = type_ == GDriveFileType.FILE
        query = ('not ' if files_only else '') + 'mimeType = "application/vnd.google-apps.folder"'
        queries.append(query)

    if mimes:
        query = ' or '.join(['mimeType = "{}"'.format(mime) for mime in mimes])
        queries.append(query)

    if name:
        queries.append('name = "{}"'.format(name))

    if parent_id:
        queries.append('"{}" in parents'.format(parent_id))

    return ' and '.join(['({})'.format(q) for q in queries if q])


class GDriveService(object):
    """Wrapper for Google Drive API.

    Provides simple functions to find files in a hierarchy and update files attributes.
    Drive API also includes support for Docs/Slides/Sheets API by default.
    To edit Docs/Slides/Sheets, ensure API access to the relevant services has been enabled at
    https://console.developers.google.com/

    NOTE This class authenticates via service accounts (only Jean has access now).
    There is no API enabled for Docs/Slides/Sheets for the provided service credentials below.
    As a workaround, use the derived `GDriveServiceWebAuth` class instead which authenticates via
    client secrets and allows for editing Docs/Slides/Sheets.

    TODO To enable Sheets API, visit the following (with an admin account):
    https://console.developers.google.com/apis/api/sheets.googleapis.com/overview?project=108212984800

    Reference:
        - https://developers.google.com/api-client-library/python/auth/service-accounts
        - https://developers.google.com/api-client-library/python/auth/web-app
        - https://developers.google.com/api-client-library/python/guide/aaa_client_secrets

    """

    DEFAULT_FILE_FIELDS = 'parents, mimeType, id, name'
    # google's api supports data retrieval from both teamdrive  and mydrive
    # even when TEAM_DRIVE_ENABLED is True
    TEAM_DRIVE_ENABLED = True

    def __init__(self):
        settings = Settings()
        validate_settings(
            'GOOGLE_DRIVE_DEFAULT_USER', 'GOOGLE_DRIVE_PRIVATE_KEY', 'GOOGLE_DRIVE_PRIVATE_KEY_ID'
        )
        credentials = self._build_credentials(
            settings['GOOGLE_DRIVE_DEFAULT_USER'], self._json_credentials(settings)
        )
        # the api only support this http lib but it probably mess how Scrapy
        # schedules the requests, making them sequential and threfor a lot
        # slower. To keep in mind when we will migrate a lot of this type of
        # scraper.
        self.session = credentials.authorize(Http())
        self._drive = self._build_service(self.session, 'drive', 'v3')
        self._sheets = self._build_service(self.session, 'sheets', 'v4')

    @staticmethod
    def _json_credentials(settings):
        return {
            'type': 'service_account',
            'project_id': 'data-ship-loader',
            'private_key_id': settings['GOOGLE_DRIVE_PRIVATE_KEY_ID'],
            'private_key': settings['GOOGLE_DRIVE_PRIVATE_KEY'],
            'client_email': 'ship-load@data-ship-loader.iam.gserviceaccount.com',
            'client_id': '114476657157219896450',
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://accounts.google.com/o/oauth2/token',
            'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
            'client_x509_cert_url': (
                'https://www.googleapis.com/robot/v1/metadata/x509/ship-load%'
                '40data-ship-loader.iam.gserviceaccount.com'
            ),
        }

    @staticmethod
    def _build_credentials(user, key):
        # see https://developers.google.com/identity/protocols/googlescopes
        scopes = ['https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(key, scopes=scopes)

        return credentials.create_delegated(user)

    @staticmethod
    @retry(tries=4)
    def _build_service(session, service, version):
        """Wrap official with retries (4 seems empirically big enough).
        """
        return build(service, version, http=session, cache_discovery=False)

    def list_files(self, type_=None, name=None, mimes=None, query=None, page_size=1000):
        """List all files matching the arguments.

        Args:
            type_ (str | unicode): Type of the files, FILE or FOLDER (use GDriveFileType)
            name (str | unicode): Name of the files
            query (str | unicode): Additional query to use to filter the files
            page_size (int): limit of files par page/call

        """
        full_query = build_query(query, type_=type_, name=name, mimes=mimes)
        results = (
            self._drive.files()
            .list(
                q=full_query,
                fields='files({})'.format(self.DEFAULT_FILE_FIELDS),
                pageSize=page_size,
                orderBy='modifiedTime desc',
                supportsTeamDrives=self.TEAM_DRIVE_ENABLED,
                includeTeamDriveItems=self.TEAM_DRIVE_ENABLED,
            )
            .execute()
        )
        return results['files']

    def list_children(self, folder_id, type_=None, name=None, mimes=None, query=None):
        """List all children of a folder, filtered by type, name.

        Args:
            folder_id (str | unicode)
            type_ (str | unicode): Type of the files, FILE or FOLDER (use GDriveFileType)
            name (str | unicode): Name of the files
            query (str | unicode): Additional query to use to filter the files

        Returns:
            list[dict]:

        """
        return self.list_files(
            type_=type_, name=name, mimes=mimes, query=build_query(query, parent_id=folder_id)
        )

    def get_file_by_id(self, file_id):
        """Retrieve a file, given its unique id.

        Args:
            file_id (str | unicode)

        Returns:
            dict

        """
        return (
            self._drive.files()
            .get(
                fileId=file_id,
                fields=self.DEFAULT_FILE_FIELDS,
                supportsTeamDrives=self.TEAM_DRIVE_ENABLED,
            )
            .execute()
        )

    def list_files_in_path(self, folder_id, path, recursive=False, query=None):
        """List file metadata located at target path, given root folder_id.

        Args:
            folder_id (str | unicode): id of the root folder to start from
            path (str | unicode): path from the root folder to the target folder
            recursive (bool): parse target folder and its subdirectories recursively
            query (str | unicode): query to use to filter files

        Returns:
            list[dict]: list of dictionary of file name/id and gdrive metadata

        """
        current_folder_id = folder_id
        # obtain target path's folder_id
        # NOTE because google's api does not support direct retrieval of a nested dir,
        # we will have to iterate across each successive subdir until we reach the layer we target
        for child_folder_name in path.split('/'):
            child_folder = self.list_children(
                folder_id=current_folder_id, type_=GDriveFileType.FOLDER, name=child_folder_name
            )
            if child_folder:
                current_folder_id = child_folder[0]['id']

        files = []
        folder_ids_stack = [current_folder_id]
        # recursively get descendant files within target folder_id using depth-first search
        while folder_ids_stack:
            current_folder_id = folder_ids_stack.pop()
            files += self.list_children(
                query=query, folder_id=current_folder_id, type_=GDriveFileType.FILE
            )
            if recursive:
                folder_ids_stack += [
                    folder['id']
                    for folder in self.list_children(
                        folder_id=current_folder_id, type_=GDriveFileType.FOLDER
                    )
                ]
        return files

    def tag_file(self, file_id, tags):
        """Add tags a file, given its id.

        Args:
            file_id (str | unicode)
            tags (list[(str | unicode)])

        """
        self._set_tags(file_id, tags, value='True')

    def untag_file(self, file_id, tags):
        """Remove tags  from a file, given its id.

        Args:
            file_id (str | unicode)
            tags (list[(str | unicode)])

        """
        self._set_tags(file_id, tags, value=None)

    def _set_tags(self, file_id, tags, value):
        """Set the tags of the specified file to the provided value.

        Args:
            file_id (str | unicode)
            tags (list[(str | unicode)])
            value (str | unicode | None)

        """
        tags_dict = {tag: value for tag in tags}
        self._drive.files().update(
            fileId=file_id,
            body={'properties': tags_dict},
            supportsTeamDrives=self.TEAM_DRIVE_ENABLED,
        ).execute()

    def create(self, name, mimetype, **kwargs):
        """Create an empty object on Drive.

        This function is particularly useful if we want to initialise an empty
        Google Sheet/Doc/Slide that will not be possible with a simple upload.

        For more details:
        https://developers.google.com/api-client-library/python/guide/media_upload

        Args:
            name (str): name of file
            mimetype (str):

        Returns:
            Dict[str, str]: metadata dict of newly created file

        """
        metadata = {'name': name, 'mimeType': mimetype}
        # to specify the directory the file is uploaded in, see:
        # https://developers.google.com/drive/v3/web/folder#inserting_a_file_in_a_folder
        metadata.update(kwargs)
        return (
            self._drive.files()
            .create(body=metadata, supportsTeamDrives=self.TEAM_DRIVE_ENABLED)
            .execute()
        )

    def upload(self, local_path, mime_type, target_name=None, target_folder_id=None):
        """Upload an existing file onto Google Drive.
        """
        file_metadata = {'name': target_name or os.path.basename(local_path), 'mimeType': mime_type}
        if target_folder_id:
            file_metadata['parents'] = [target_folder_id]

        media = MediaFileUpload(local_path, mimetype=mime_type)
        return (
            self._drive.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields='parents, mimeType, id, name',
                supportsTeamDrives=self.TEAM_DRIVE_ENABLED,
            )
            .execute()
        )

    def download_file(self, item, target_path):
        """Downloads a drive file's content to the local file system.

        Args:
            item (dict): gdrive file dict
            target_path (str | unicode): path to the target file

        """
        mime_type = item['mimeType']
        open_mode = 'wb' if mime_type in GDriveMimeTypes.PDF else 'w'

        with open(target_path, open_mode) as fd:
            fd.write(self.fetch_file_content(item))

    def fetch_file_content(self, item):
        mime_type = item['mimeType']
        if mime_type in GDriveMimeTypes.PDF:
            _, content = self.session.request(PDF_DOWNLOAD_URL_TPL.format(id_=item['id']))
        elif mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            content = (
                self._drive.files()
                .get_media(fileId=item['id'], supportsTeamDrives=self.TEAM_DRIVE_ENABLED)
                .execute()
            )
        else:
            if mime_type in GDriveMimeTypes.SPREADSHEETS:
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            else:
                mime_type = 'text/plain'
            req = self._drive.files().export_media(fileId=item['id'], mimeType=mime_type)
            content = req.execute()
        return content

    def move(self, id, parent_id):
        """Move an object from one parent directory to another.

        Args:
            id (str): object to move
            parent_id (str): new parent folder id

        Returns:
            Dict[str, str]: updated metadata dict of moved file

        """
        # retrieve existing parent directory to remove object from
        file = (
            self._drive.files()
            .get(fileId=id, fields='parents', supportsTeamDrives=self.TEAM_DRIVE_ENABLED)
            .execute()
        )
        # move selected object to be a descendant of the parent object
        return (
            self._drive.files()
            .update(
                fileId=id,
                addParents=parent_id,
                removeParents=','.join(file.get('parents', '')),
                fields='id, parents',
                supportsTeamDrives=self.TEAM_DRIVE_ENABLED,
            )
            .execute()
        )

    def delete(self, id):
        """Delete specified file/folder in GDrive.

        Args:
            id (str): id of file/folder

        Returns:
            Dict[str, str]: metadata dict of removed object

        """
        return (
            self._drive.files()
            .delete(fileId=id, supportsTeamDrives=self.TEAM_DRIVE_ENABLED)
            .execute()
        )


class GDriveServiceWebAuth(GDriveService):
    """Derived wrapper for Google Drive API.

    Provides simple functions to find files in a hierarchy and update files attributes.
    Drive API also includes support for Docs/Slides/Sheets API by default.

    NOTE This class authenticates via client secrets as a workaround.
    Therefore, we have full API edit access to Docs/Slides/Sheets.

    Reference:
        - https://developers.google.com/api-client-library/python/auth/web-app
        - https://developers.google.com/api-client-library/python/guide/aaa_client_secrets

    """

    def __init__(self):
        settings = Settings()
        validate_settings(
            'GMAIL_CLIENT_SECRET',
            'GMAIL_ACCESS_TOKEN',
            'GMAIL_CLIENT_ID',
            'GMAIL_REFRESH_TOKEN',
            'GMAIL_TOKEN_EXPIRY',
        )
        credentials = self._build_credentials(settings)
        self.session = credentials.authorize(Http())
        self._drive = self._build_service(self.session, 'drive', 'v3')
        self._sheets = self._build_service(self.session, 'sheets', 'v4')

    def _build_credentials(self, settings):
        self._save_file(json.dumps(self._client_secret(settings)), 'client_secret.json')
        self._save_file(json.dumps(self._auth_token(settings)), 'auth_token.json')

        # for more details, see:
        # https://developers.google.com/identity/protocols/googlescopes
        scopes = ['https://www.googleapis.com/auth/drive']

        # try and get cached access keys, if it is present
        store = file.Storage(os.path.join(os.getcwd(), 'auth_token.json'))
        creds = store.get()
        # if not present, generate access keys from client secret file
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets(
                os.path.join(os.getcwd(), 'client_secret.json'), scopes
            )
            creds = tools.run_flow(flow, store)
        return creds

    @staticmethod
    def _save_file(body, name):
        full_path = os.path.join(os.getcwd(), name)
        open(full_path, 'w').write(body)

    @staticmethod
    def _auth_token(settings):
        return {
            '_class': 'OAuth2Credentials',
            '_module': 'oauth2client.client',
            'access_token': settings['GMAIL_ACCESS_TOKEN'],
            'client_id': '{}.apps.googleusercontent.com'.format(settings['GMAIL_CLIENT_ID']),
            'client_secret': settings['GMAIL_CLIENT_SECRET'],
            'id_token': None,
            'id_token_jwt': None,
            'invalid': False,
            'refresh_token': settings['GMAIL_REFRESH_TOKEN'],
            'revoke_uri': 'https://accounts.google.com/o/oauth2/revoke',
            'scopes': ['https://www.googleapis.com/auth/drive'],
            'token_expiry': settings['GMAIL_TOKEN_EXPIRY'],
            'token_info_uri': 'https://www.googleapis.com/oauth2/v3/tokeninfo',
            'token_response': {
                'access_token': settings['GMAIL_ACCESS_TOKEN'],
                'expires_in': 3600,
                'token_type': 'Bearer',
            },
            'token_uri': 'https://accounts.google.com/o/oauth2/token',
            'user_agent': None,
        }

    @staticmethod
    def _client_secret(settings):
        return {
            'installed': {
                'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
                'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                'client_id': '{}.apps.googleusercontent.com'.format(settings['GMAIL_CLIENT_ID']),
                'client_secret': settings['GMAIL_CLIENT_SECRET'],
                'project_id': 'kpler-lab',
                'redirect_uris': ['urn:ietf:wg:oauth:2.0:oob', 'http://localhost'],
                'token_uri': 'https://accounts.google.com/o/oauth2/token',
            }
        }


class GSheetsService(GDriveServiceWebAuth):
    """Derived wrapper for Google Sheets API.

    Provides simple functions to get/edit Google Sheets.
    Drive API comes with read-only access to Docs/Slides/Sheets by default.

    NOTE This class authenticates via client secrets as a workaround for the current
    service account not having Google Sheets API enabled (i.e., no edit access).

    Reference:
        - https://developers.google.com/api-client-library/python/auth/web-app
        - https://developers.google.com/api-client-library/python/guide/aaa_client_secrets

    """

    def __init__(self):
        # we need the self.session attribute to build Sheets service
        super(GSheetsService, self).__init__()
        self._sheets = self._build_service(self.session, 'sheets', 'v4')

    def read_sheet(self, file_id, sheet_names=None):
        """Read contents of spreadsheet on Drive.
        """
        file_item = self.fetch_by_id(file_id)
        content = self.fetch_file_content(file_item)
        workbook = Workbook(content=content)

        valid_sheets = (s for s in workbook if not sheet_names or s.name in sheet_names)
        return {sheet.name: [item for item in sheet.items] for sheet in valid_sheets}

    def clear_sheet(self, file_id):
        """Clear all cell values from a Google Sheet.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear

        Args:
            file_id (str): unique file id (can be obtained from url)

        Returns:
            Dict[str, str] | None: status dict of spreadsheet if cleared successfully

        """
        return (
            self._sheets.spreadsheets()
            .values()
            .clear(
                spreadsheetId=file_id,
                # range is in A1 notation
                # https://developers.google.com/sheets/api/guides/values
                range=ALL_CELLS,
                body={},
            )
            .execute()
        )

    def write_sheet(self, file_id, rows, valueInputOption='RAW', **kwargs):
        """Update Google Sheet with specified rows.

        https://developers.google.com/sheets/api/guides/values#writing
        https://developers.google.com/sheets/api/samples/writing

        Args:
            id (str): unique file id (can be obtained from url)
            rows (List[List[str]]): rows of data to insert

        Returns:
            Dict[str, str] | None: status dict of newly updated file if updated successfully

        """
        if not rows:
            # no data to insert in spreadsheet
            return

        # range is in A1 notation
        # https://developers.google.com/sheets/api/guides/values
        # `ZZ` will just push the items to all the place available
        body = {'majorDimension': 'ROWS', 'values': rows}
        return (
            self._sheets.spreadsheets()
            .values()
            .update(
                spreadsheetId=file_id,
                range=ALL_CELLS,
                body=body,
                # 'valueInputOption' will control auto-formatting of strings inside spreadsheet
                # https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption
                valueInputOption=valueInputOption,
                **kwargs,
            )
            .execute()
        )
