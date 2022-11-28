#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno

from fuse import FUSE, FuseOSError, Operations, fuse_get_context

from drive_client import list_dir_manipulated

token = '{"access_token": "ya29.a0Aa4xrXMNAdNSTGuBeRV-OblCZHE8HNAEmmtlQ2beBGbbRFhm8PizfGFGnx-0PYQlrdmaVxlUQVDlKN0ANkvtfAUG3TMaEv31xyVMorOkuOQgftcYyRyIiHCdyDhI-3rKjvRUZGi6774WLZ2iidD8pUa2JPhRaCgYKATASARASFQEjDvL9mQNjn4Ut7avEd5MIdvyBfA0163", "token_type": "Bearer", "refresh_token": "1//04OjNYdknXPrTCgYIARAAGAQSNwF-L9IrGOKvOgbfTkeKMxWeY90BoxLJLxpJuW1CUWKmAJsqXQTuxZUCgXXy8mQbICiwQmvBwFI", "expiry": "2022-10-10T16:26:24.445819Z"}'
client_id = '561906364179-jrl0tnvchd73c9tsvppfnjllursdff1t.apps.googleusercontent.com'
client_secret = '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'


def log(func):
    def decorator_handler(*args, **kwargs):
        print(func.__name__)
        return func(*args, **kwargs)

    return decorator_handler


def my_test(file_path, seek_bytes_from, total_bytes_read):
    a = open('/home/mastersesin12345/root' + file_path, 'rb')
    a.seek(seek_bytes_from)
    data = a.read(total_bytes_read)
    a.close()
    return data


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root

    # Helpers
    # =======
    @log
    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        print(path)
        return path

    # Filesystem methods
    # ==================
    @log
    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    @log
    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    @log
    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    @log
    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        a = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                     'st_uid'))
        # {'st_atime': 1669403579.5569782, 'st_ctime': 1669403567.356995, 'st_gid': 1002, 'st_mode': 16893,
        #            'st_mtime': 1669403567.356995, 'st_nlink': 2, 'st_size': 4096, 'st_uid': 1001}
        if full_path.endswith('.json'):
            return {'st_atime': 1669526320, 'st_ctime': 1669526098, 'st_gid': 1002, 'st_mode': 33188,
                    'st_mtime': 1669320754, 'st_nlink': 1, 'st_size': 108830059313, 'st_uid': 1001}
        return a

    @log
    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        dirents.extend([k for k, v in list_dir_manipulated(token, client_id, client_secret,
                                                           "1T2erq7cVOOo3ZpZN6BeE0WBsrXlsVrwN").items()])
        for r in dirents:
            yield r

    @log
    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    @log
    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    @log
    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    @log
    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    @log
    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    @log
    def unlink(self, path):
        return os.unlink(self._full_path(path))

    @log
    def symlink(self, name, target):
        return os.symlink(target, self._full_path(name))

    @log
    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    @log
    def link(self, target, name):
        return os.link(self._full_path(name), self._full_path(target))

    @log
    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============
    @log
    def open(self, path, flags):
        path = '/home/mastersesin12345/run.py'
        full_path = self._full_path(path)
        return os.open(path, flags)

    @log
    def create(self, path, mode, fi=None):
        uid, gid, pid = fuse_get_context()
        full_path = self._full_path(path)
        fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
        os.chown(full_path, uid, gid)  # chown to context uid & gid
        return fd

    @log
    # plot-k32-2022-11-24-18-08-01588da0c4d62d03dab8f700daddbb7aed3e6868f0a567d02772c8856f26618a.plot
    def read(self, path, length, offset, fh):
        print(length, offset)
        bytes_read = my_test(path, offset, length)
        return bytes_read

    @log
    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    @log
    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    @log
    def flush(self, path, fh):
        return os.fsync(fh)

    @log
    def release(self, path, fh):
        return os.close(fh)

    @log
    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
