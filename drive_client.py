import io
import json
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from cryptography.fernet import Fernet

EK = b'E-bxU5geNyrojsSg2mqn5Yv1_veAczf0xaffrFJBSjk='
FERNET_OBJ = Fernet(EK)

DEFAULT_DRIVE = "0AGLae2SSKNqsUk9PVA"

proceed_file = {}
map_file_id = {}


def upload(file_in_mem, token, file_name, client_id, client_secret, drive_id_or_folder_id=DEFAULT_DRIVE):
    print(f'Uploading {file_name}')
    creds = Credentials.from_authorized_user_info(
        info={**json.loads(token), 'client_id': client_id, 'client_secret': client_secret}
    )
    service = build('drive', 'v3', credentials=creds)
    media = MediaIoBaseUpload(
        io.BytesIO(file_in_mem),
        mimetype='application/octet-stream',
        resumable=True,
        chunksize=512 * 1024 * 1024
    )
    request = service.files().create(
        media_body=media,
        body={'name': file_name, 'parents': [drive_id_or_folder_id]},
        supportsAllDrives=True
    )
    response = None
    while response is None:
        status, response = request.next_chunk()
    del file_in_mem
    del media
    return True


def list_dir(token, client_id, client_secret, folder_id, next_page_token=None):
    files = []
    try:
        creds = Credentials.from_authorized_user_info(
            info={**json.loads(token), 'client_id': client_id, 'client_secret': client_secret}
        )
        service = build('drive', 'v3', credentials=creds)
        response = service.files().list(q=f'parents in "{folder_id}"and trashed=false',
                                        spaces='drive',
                                        fields='nextPageToken, '
                                               'files(id, name)',
                                        pageToken=next_page_token,
                                        supportsAllDrives=True,
                                        includeItemsFromAllDrives=True).execute()
        files.extend(response.get('files', []))
        next_page_token = response.get('nextPageToken', None)
        if next_page_token:
            files.extend(list_dir(token, client_id, client_secret, folder_id, next_page_token))

    except Exception as error:
        print(f'{error} - will retry')
        files.extend(list_dir(token, client_id, client_secret, folder_id, next_page_token))

    return files


def merge_split_file_into_one(dict_file, token, client_id, client_secret):
    creds = Credentials.from_authorized_user_info(
        info={**json.loads(token), 'client_id': client_id, 'client_secret': client_secret}
    )
    list_json_file = [k for k, v in dict_file.items() if k.endswith('.json')]
    return_dict = {}
    for json_file_name in list_json_file:
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=dict_file[json_file_name])
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        return_dict[json_file_name] = json.loads(FERNET_OBJ.decrypt(file.getvalue()).decode('ascii'))
    return return_dict


def list_dir_manipulated(token, client_id, client_secret, folder_id):
    global proceed_file
    global map_file_id
    file_list_before = list_dir(token, client_id, client_secret, folder_id)
    for item in file_list_before:
        map_file_id[item['name']] = item['id']
    proceed_file.update(
        merge_split_file_into_one(dict_file=map_file_id, token=token, client_id=client_id,
                                  client_secret=client_secret)
    )
    return proceed_file


def read_file_in_chunk(token, client_id, client_secret, file_id, offset, length):
    try:
        creds = Credentials.from_authorized_user_info(
            info={**json.loads(token), 'client_id': client_id, 'client_secret': client_secret}
        )
        service = build('drive', 'v3', credentials=creds)
        request = service.files().get_media(fileId=file_id)
        request.headers["Range"] = "bytes={}-{}".format(offset, offset + length)
        fh = io.BytesIO(request.execute())

    except Exception as error:
        print(F'An error occurred: {error}')
        return read_file_in_chunk(token, client_id, client_secret, file_id, offset, length)

    return fh.getvalue()


if __name__ == '__main__':
    token = '{"access_token": "ya29.a0Aa4xrXMNAdNSTGuBeRV-OblCZHE8HNAEmmtlQ2beBGbbRFhm8PizfGFGnx-0PYQlrdmaVxlUQVDlKN0ANkvtfAUG3TMaEv31xyVMorOkuOQgftcYyRyIiHCdyDhI-3rKjvRUZGi6774WLZ2iidD8pUa2JPhRaCgYKATASARASFQEjDvL9mQNjn4Ut7avEd5MIdvyBfA0163", "token_type": "Bearer", "refresh_token": "1//04OjNYdknXPrTCgYIARAAGAQSNwF-L9IrGOKvOgbfTkeKMxWeY90BoxLJLxpJuW1CUWKmAJsqXQTuxZUCgXXy8mQbICiwQmvBwFI", "expiry": "2022-10-10T16:26:24.445819Z"}'
    client_id = '561906364179-jrl0tnvchd73c9tsvppfnjllursdff1t.apps.googleusercontent.com'
    client_secret = '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'
    # # print(list_dir(token=token,
    # #                folder_id='1T2erq7cVOOo3ZpZN6BeE0WBsrXlsVrwN', client_id=client_id,
    # #                client_secret=client_secret))
    # # print('done')
    # list_dir_manipulated(token=token, client_id=client_id, client_secret=client_secret,
    #                      folder_id='1T2erq7cVOOo3ZpZN6BeE0WBsrXlsVrwN')
    # print(proceed_file)
    # print(map_file_id)
    # print(read_file_in_chunk(token=token, client_id=client_id, client_secret=client_secret,
    #                          file_id='1iAmwI3sOHGWlZfdhQYqZ3_OOBdtzgPvq', offset=5, length=5))
    dirents = ['.', '..']
    dirents.extend([k for k, v in list_dir_manipulated(token, client_id, client_secret, "1T2erq7cVOOo3ZpZN6BeE0WBsrXlsVrwN").items()])
    print(dirents)
