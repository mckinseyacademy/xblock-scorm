import os
import tempfile
import re
import zipfile
import shutil
import logging

from django.conf import settings
from django.core.files.storage import default_storage
from django.core.files.storage import get_storage_class
from django.utils import encoding
from django.core.cache import cache


logger = logging.getLogger(__name__)

# Regex to capture Content-Range header ranges.
CONTENT_RE = re.compile(r"(?P<start>\d{1,11})-(?P<stop>\d{1,11})/(?P<end>\d{1,11})")


class STATES(object):
    """
    enum for upload state
    """
    progress = 'PROGRESS'
    error = 'ERROR'
    complete = 'COMPLETE'


class ScormPackageUploader(object):
    """
    Handles scorm package uploading
    """
    def __init__(self, request, xblock, scorm_storage_location):
        self.xblock = xblock
        self.request = request
        self.scorm_file = request.params['scorm_file'].file
        self.temp_file_path = os.path.join(tempfile.gettempdir(), xblock.location.block_id)
        self.scorm_storage_location = os.path.join(scorm_storage_location, xblock.location.block_id)

    def upload(self):
        content_range = self._get_content_range()

        if int(content_range['start']) == 0:
            mode = "wb+"
        else:
            mode = "ab+"
            size = os.path.getsize(self.temp_file_path)
            if size > int(content_range['stop']) and size == int(content_range['end']):
                return STATES.complete, None

        self._write_to_file(mode=mode)
        size = os.path.getsize(self.temp_file_path)

        if int(content_range['stop']) != int(content_range['end']) - 1:
            # More chunks coming
            return STATES.progress, size

        scorm_file_url = self._extract_and_store()

        return STATES.complete, scorm_file_url

    def _extract_and_store(self):
        cache.set('upload_percent_{}'.format(self.xblock.location.block_id), 0, 1 * 60 * 60)
        unizpped_dir = self._extract_zipped_file()
        storage_url = self._save_to_storage(unizpped_dir)

        return storage_url

    def _save_to_storage(self, tempdir):
        storage = self._get_storage()
        self._cleanup_storage_dir(storage)

        to_store = []
        total_size = 0
        for (dirpath, dirnames, files) in os.walk(tempdir):
            for f in files:
                file_path = os.path.join(os.path.abspath(dirpath), f)
                size = os.path.getsize(file_path)
                total_size += size
                to_store.append({'path': file_path, 'size': size})

        uploaded_size = 0
        for f in to_store:
            # defensive decode/encode from zip
            file_path = f['path']
            f_path = file_path.decode(self.xblock.encoding).encode('utf-8').replace(tempdir, '')
            with open(file_path, 'rb+') as fh:
                try:
                    logger.info('Storing file `{}` of size `{}` on S3'.format(f_path, f['size']))
                    storage.save('{}{}'.format(self.scorm_storage_location, f_path), fh)
                    logger.info('File `{}` stored.'.format(f_path))
                    uploaded_size += f['size']
                    self._set_upload_progress(uploaded_size, total_size)
                except encoding.DjangoUnicodeDecodeError, e:
                    logger.warn('SCORM XBlock Couldn\'t store file {} to storage. {}'.format(f, e))

        self._post_upload_cleanup(tempdir)

        url = storage.url(self.scorm_storage_location)
        return '?' in url and url[:url.find('?')] or url

    def _set_upload_progress(self, uploaded, total):
        percent = int(uploaded / float(total) * 100)
        block_id = self.xblock.location.block_id

        cache.set('upload_percent_{}'.format(block_id), percent, 1 * 60 * 60)

    def _post_upload_cleanup(self, tempdir):
        try:
            shutil.rmtree(tempdir)
            os.remove(self.temp_file_path)
        except Exception:
            pass

    def _get_storage(self):
        if settings.DEFAULT_FILE_STORAGE == 'storages.backends.s3boto.S3BotoStorage':
            s3_boto_storage_class = get_storage_class()
            # initializing S3 storage with private acl
            storage = s3_boto_storage_class(acl='private')
        else:
            storage = default_storage

        return storage

    def _cleanup_storage_dir(self, storage):
        if storage.exists(os.path.join(self.scorm_storage_location, 'imsmanifest.xml')):
            try:
                shutil.rmtree(os.path.join(storage.location, self.scorm_storage_location))
            except OSError:
                # TODO: for now we are going to assume this means it's stored on S3 if not local
                try:
                    for key in storage.bucket.list(prefix=self.scorm_storage_location):
                        key.delete()
                except AttributeError:
                    raise

    def _extract_zipped_file(self):
        zip_file = zipfile.ZipFile(self.temp_file_path, 'r')
        tempdir = tempfile.mkdtemp()
        zip_file.extractall(tempdir)

        return tempdir

    def _write_to_file(self, mode):
        with open(self.temp_file_path, mode) as temp_file:
            for chunk in self.scorm_file.chunks():
                temp_file.write(chunk)

    def _get_content_range(self):
        try:
            matches = CONTENT_RE.search(self.request.headers['Content-Range'])
            content_range = matches.groupdict()
        except KeyError:  # Single chunk
            # no Content-Range header, so make one that will work
            content_range = {'start': 0, 'stop': 1, 'end': 2}

        return content_range

    @staticmethod
    def get_upload_percentage(block_id):
        return cache.get('upload_percent_{}'.format(block_id), 100)

    @staticmethod
    def clear_percentage_cache(block_id):
        cache.delete('upload_percent_{}'.format(block_id))
