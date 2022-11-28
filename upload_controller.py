import json
import random
import threading
import time
import uuid

from cryptography.fernet import Fernet

from drive_client import upload

MAX_FILE_SIZE = 3 * 1024 * 1024 * 1024
MIN_FILE_SIZE = 2 * 1024 * 1024 * 1024
MAX_THREAD = 5
ENCRYPTED_TOTAL_BYTES = 1
RANDOM_FILE_EXTENSION_LIST = ['pak', 'bak', 'arc', 'arj', 'bin', 'dump']
EK = b'E-bxU5geNyrojsSg2mqn5Yv1_veAczf0xaffrFJBSjk='
FERNET_OBJ = Fernet(EK)


def read_in_chunks(file_obj):
    should_encrypted_byte = file_obj.read(ENCRYPTED_TOTAL_BYTES)
    yield FERNET_OBJ.encrypt(should_encrypted_byte), 0, ENCRYPTED_TOTAL_BYTES
    start_byte = ENCRYPTED_TOTAL_BYTES
    while True:
        current_start_byte = start_byte
        bytes_read = random.randint(MIN_FILE_SIZE, MAX_FILE_SIZE)
        data = file_obj.read(bytes_read)
        end_byte = start_byte + bytes_read
        next_start_byte = start_byte + bytes_read
        start_byte = next_start_byte + 1
        if not data:
            break
        yield data, current_start_byte, end_byte


def main(plot_file_abs_path):
    token = '{"access_token": "ya29.a0Aa4xrXMNAdNSTGuBeRV-OblCZHE8HNAEmmtlQ2beBGbbRFhm8PizfGFGnx-0PYQlrdmaVxlUQVDlKN0ANkvtfAUG3TMaEv31xyVMorOkuOQgftcYyRyIiHCdyDhI-3rKjvRUZGi6774WLZ2iidD8pUa2JPhRaCgYKATASARASFQEjDvL9mQNjn4Ut7avEd5MIdvyBfA0163", "token_type": "Bearer", "refresh_token": "1//04OjNYdknXPrTCgYIARAAGAQSNwF-L9IrGOKvOgbfTkeKMxWeY90BoxLJLxpJuW1CUWKmAJsqXQTuxZUCgXXy8mQbICiwQmvBwFI", "expiry": "2022-10-10T16:26:24.445819Z"}'
    client_id = '561906364179-jrl0tnvchd73c9tsvppfnjllursdff1t.apps.googleusercontent.com'
    client_secret = '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'
    list_thread = []
    chunk_detail = {}
    file_obj = open(plot_file_abs_path, 'rb')
    for chunk, start_byte, end_byte in read_in_chunks(file_obj):
        file_name = f'{uuid.uuid4()}.{random.choice(RANDOM_FILE_EXTENSION_LIST)}'
        chunk_detail.update({f'{end_byte}': file_name})
        worker = threading.Thread(target=upload, kwargs={
            'file_in_mem': chunk,
            'token': token,
            'client_id': client_id,
            'client_secret': client_secret,
            'file_name': file_name
        })
        worker.start()
        list_thread.append(worker)
        while len(list_thread) >= MAX_THREAD:
            list_thread = [running_process for running_process in list_thread if running_process.is_alive()]
            time.sleep(0.01)
    for thread in list_thread:
        thread.join()
    threading.Thread(target=upload, kwargs={
        'file_in_mem': FERNET_OBJ.encrypt(json.dumps(chunk_detail).encode()),
        'token': token,
        'client_id': client_id,
        'client_secret': client_secret,
        'file_name': f'{str(uuid.uuid4())}.json'
    }).start()


if __name__ == '__main__':
    main("/tmp1/plot-k32-2022-11-28-05-04-89656e4c3fea9f3763a0513f8b185786832a4719cc0c73a738dafabc762695b8.plot")
