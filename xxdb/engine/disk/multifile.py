import asyncio
from pathlib import Path

from .disk import Disk
from .page import Page
from .blockio import BlockIO


class MultiFile(Disk):
    PAGEID_SIZE = 4
    # SINGLE_FILE_SIZE = 64 * 1024 * 1024  # 64MB

    def __init__(self, name, data_dpath, config):
        self._name = name
        self._data_dpath = Path(data_dpath)
        self._config = config
        self._BLOCK_SIZE = self._config.params['block_size'] * 1024 * 1024  # MB

        self._page_size = self._config.page_size
        self._page_per_file = self._BLOCK_SIZE // self._page_size
        _block_fpath_list = list(self._data_dpath.glob(f'{self._name}.*.dat.xxdb'))
        _block_fpath_list.sort(key=lambda fpath: int(fpath.name.split('.')[1]))
        self._block_list = [BlockIO(fpath, self._page_size) for fpath in _block_fpath_list]
        self._next_pageid = sum(bio.page_len for bio in self._block_list)

    @property
    def gen_empty_page(self):
        return b'\x00' * self._page_size

    @property
    def pageid_size(self):
        return self.PAGEID_SIZE

    def _get_bio(self, blockid: int):
        if blockid >= len(self._block_list):
            print((blockid, len(self._block_list)), flush=True)
            bio_fpath = self._data_dpath / f"{self._name}.{blockid}.dat.xxdb"
            bio = BlockIO(bio_fpath, self._page_size)
            self._block_list.append(bio)
        else:
            bio = self._block_list[blockid]
        return bio

    def _calc_offset(self, pageid):
        blockid, partial_pageid = divmod(pageid, self._page_per_file)
        return (blockid, partial_pageid)

    async def flush(self):
        [_bio.flush() for _bio in self._block_list]

    async def close(self):
        await self.flush()
        print(self._next_pageid, flush=True)
        print([bio.page_len for bio in self._block_list], flush=True)
        [_bio.close() for _bio in self._block_list]

    def new_page(self) -> Page:
        pageid = self._next_pageid
        blockid, part_pageid = self._calc_offset(pageid)
        if part_pageid == 0:
            # init bio (create file)
            _ = self._get_bio(blockid)
            print(f"new block: {blockid}", flush=True)
        self._next_pageid += 1
        page_data = self.gen_empty_page
        return Page(page_data, pageid)

    async def read_page(self, pageid: int) -> Page:
        blockid, part_pageid = self._calc_offset(pageid)
        bio = self._get_bio(blockid)
        try:
            # return Page(bio.seek_read1(part_pageid), pageid)
            page_data = await asyncio.to_thread(bio.seek_read1, part_pageid)
            return Page(page_data, pageid)
        except Exception as e:
            print(f"block: {blockid}")
            raise e

    async def write_page(self, page: Page) -> None:
        pageid = page.id
        blockid, part_pageid = self._calc_offset(pageid)
        bio = self._get_bio(blockid)
        await asyncio.to_thread(bio.seek_write, part_pageid, page.dumps_page())
        # bio.seek_write(part_pageid, page.dumps_page())
        bio.flush()
