import io
import os
import pickle
from pathlib import Path
from typing import IO

import indexed_bzip2 as ibz2
from py7zr import SevenZipFile, is_7zfile
from indexed_bzip2 import IndexedBzip2File
from loguru import logger

class MagicStepIO(io.FileIO):
    left_step_len = 0
    bytes_magic = b"BZh91AY&SY"

    def __init__(self, *args, **kwargs):
        super(MagicStepIO, self).__init__(*args, **kwargs)

        values = super().read(1024)
        step = values.find(self.bytes_magic)
        self.left_step_len = step
        self.seek(0)

    def fileno(self) -> int:
        return -1

    def tell(self):
        # print("tell", super().tell() - self.left_step_len)
        return super().tell() - self.left_step_len

    def seek(self, __offset: int, __whence: int = 0) -> int:
        # print("seek", __offset, __whence)
        if __whence == 0:
            return super().seek(__offset + self.left_step_len, __whence) - self.left_step_len
        else:
            return super().seek(__offset, __whence) - self.left_step_len

    def read(self, __size: int = ...) -> bytes:
        # print("read", __size, self.tell(), super().tell())
        temp_bytes = super().read(__size)
        return temp_bytes


def get_archive_reader(path, filename=None) -> IO:
    if "-" in path:
        logger.info(f"Take ibz2 for {path}")
        path_obj = Path(path)
        block_offsets_index_path = f"{path_obj.parent}/{path_obj.name}-index.dat"
        file_custom_fileIO = MagicStepIO(path, 'r')

        if not os.path.exists(block_offsets_index_path):
            # index to save blocks
            reader = ibz2.open(file_custom_fileIO, parallelization=os.cpu_count())
            block_offsets = reader.block_offsets()
            with open(block_offsets_index_path, 'wb') as offsets_file:
                pickle.dump(block_offsets, offsets_file)
            reader.close()
        else:
            with open(block_offsets_index_path, 'rb') as offsets_file:
                block_offsets = pickle.load(offsets_file)

        reader = ibz2.open(file_custom_fileIO, parallelization=os.cpu_count())
        reader.set_block_offsets(block_offsets)
        return reader
    if not filename:
        raise ValueError("filename not set")
    logger.info(f"Take py7z for {path}")
    zip_file = SevenZipFile(path, 'r')
    reader = zip_file.read(targets=[filename]).get(filename)
    return reader
