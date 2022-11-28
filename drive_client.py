import io
import json

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload


def upload(file_in_mem, token, file_name, client_id, client_secret, drive_id_or_folder_id="0AGLae2SSKNqsUk9PVA"):
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

# if __name__ == '__main__':
#     token = '{"access_token": "ya29.a0Aa4xrXMNAdNSTGuBeRV-OblCZHE8HNAEmmtlQ2beBGbbRFhm8PizfGFGnx-0PYQlrdmaVxlUQVDlKN0ANkvtfAUG3TMaEv31xyVMorOkuOQgftcYyRyIiHCdyDhI-3rKjvRUZGi6774WLZ2iidD8pUa2JPhRaCgYKATASARASFQEjDvL9mQNjn4Ut7avEd5MIdvyBfA0163", "token_type": "Bearer", "refresh_token": "1//04OjNYdknXPrTCgYIARAAGAQSNwF-L9IrGOKvOgbfTkeKMxWeY90BoxLJLxpJuW1CUWKmAJsqXQTuxZUCgXXy8mQbICiwQmvBwFI", "expiry": "2022-10-10T16:26:24.445819Z"}'
#     client_id = '561906364179-jrl0tnvchd73c9tsvppfnjllursdff1t.apps.googleusercontent.com'
#     client_secret = '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'
#     main(file_in_mem=open('drive_upload.py', 'rb').read(), token=token, name='hihi',
#          drive_id_or_folder_id='1IyCN8TxuPgNu5xM4iYG0422DhZ5aeTEW', client_id=client_id, client_secret=client_secret)
#     print('done')
