import asyncio
import hashlib
import io
import os
import pickle
import queue
from asyncio import AbstractEventLoop
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import IO

import indexed_bzip2 as ibz2
from py7zr import SevenZipFile, is_7zfile

from app.utils import config
from indexed_bzip2 import IndexedBzip2File
from loguru import logger

# thread pool
thread_pools = ThreadPoolExecutor(max_workers=config.settings.count_threads)


class MagicStepIO(io.FileIO):
    """step bytes to read 7z bzip2 file"""

    left_step_len = 0
    bytes_magic = b"BZh91AY&SY"

    def __init__(self, *args, **kwargs):
        super(MagicStepIO, self).__init__(*args, **kwargs)

        values = super().read(1024)
        step = values.find(self.bytes_magic)  # TODO find magic end also
        self.left_step_len = step
        self.seek(0)

    def fileno(self) -> int:
        return -1

    def tell(self):
        return super().tell() - self.left_step_len

    def seek(self, __offset: int, __whence: int = 0) -> int:
        if __whence == 0:
            return (
                super().seek(__offset + self.left_step_len, __whence)
                - self.left_step_len
            )
        else:
            return super().seek(__offset, __whence) - self.left_step_len

    def read(self, __size: int = ...) -> bytes:
        temp_bytes = super().read(__size)
        return temp_bytes


class ArchiveFileReader:
    """Async archive reader"""

    def __init__(self, path, filename=None):
        self.pool = thread_pools
        self.loop: AbstractEventLoop = asyncio.new_event_loop()
        self.bytes_queue: queue.Queue = queue.Queue(8192)
        self.path = path
        self.filename = filename
        self.size = 0
        self.str_archive_md5 = self.archive_md5()

        if "-" in path:  # TODO regex detector
            logger.info(f"Take ibz2 for {path}")
            path_obj = Path(path)
            block_offsets_index_path = f"{path_obj.parent}/{path_obj.name}-index.dat"
            file_custom_fileIO = MagicStepIO(path, "r")

            if not os.path.exists(block_offsets_index_path):
                # index to save blocks
                reader = ibz2.open(file_custom_fileIO, parallelization=os.cpu_count())
                block_offsets = reader.block_offsets()
                with open(block_offsets_index_path, "wb") as offsets_file:
                    pickle.dump(block_offsets, offsets_file)
                reader.close()
            else:
                with open(block_offsets_index_path, "rb") as offsets_file:
                    block_offsets = pickle.load(offsets_file)

            self.reader = ibz2.open(file_custom_fileIO, parallelization=os.cpu_count())
            self.reader.set_block_offsets(block_offsets)
            self.size = self.reader.size()
        else:
            if not filename:
                raise ValueError("filename not set")
            logger.info(f"Take py7z for {path}")
            zip_file = SevenZipFile(path, "r")
            self.reader = zip_file.read(targets=[filename]).get(filename)
            self.size = self.reader.seek(0, 2)

    def _sync_readlines(self, start_bytes=0, whence=0):
        """Custom sync reader form files"""
        # TODO recheck default one ?
        start_bytes = start_bytes if start_bytes > 0 else 0
        self.reader.seek(start_bytes, whence)
        data_buffer = self.reader.read(512 * 1024)
        buffer_last = b""
        while data_buffer != b"":
            data_buffer = buffer_last + data_buffer
            data_lines = data_buffer.rsplit(b"\r\n")
            for line in data_lines:
                if line.endswith(b">"):
                    self.bytes_queue.put(line + b"\r\n")
                else:
                    buffer_last = line
            data_buffer = self.reader.read(512 * 1024)
        return

    def _sync_get(self, start: int, length: int):
        self.reader.seek(start)
        return self.reader.read(length)

    async def readlines(self, start_bytes=0, whence=0):
        loop = asyncio.get_event_loop()
        """async readlines """
        if self.bytes_queue.qsize() > 0:
            while self.bytes_queue.qsize():
                self.bytes_queue.get_nowait()

        cursor_pos = 0
        sync_future = loop.run_in_executor(
            self.pool, self._sync_readlines, start_bytes, whence
        )
        while (self.bytes_queue.qsize() != 0) or (not sync_future.done()):
            if self.bytes_queue.qsize() == 0:
                await asyncio.sleep(0)
                continue
            try:
                value_temp: bytes = self.bytes_queue.get_nowait()
                yield cursor_pos, value_temp
                cursor_pos += len(value_temp)
            except queue.Empty:
                await asyncio.sleep(0)

    async def get(self, start: int, length: int):
        loop = asyncio.get_event_loop()
        sync_future = loop.run_in_executor(self.pool, self._sync_get, start, length)
        return await sync_future

    def archive_md5(self):
        hash_md5 = hashlib.md5()
        with open(self.path, "rb") as file:
            file.seek(0)
            hash_md5.update(file.read(512 * 1024))
            file.seek((512 * 1024) * 4, 2)
            hash_md5.update(file.read(512 * 1024))
        return hash_md5.hexdigest()


def get_archive_filenames(path):
    with SevenZipFile(path, "r") as archive_read:
        all_archive_files = archive_read.getnames()
    return all_archive_files
