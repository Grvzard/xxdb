from xxdb.engine.config import DiskConfig
from .page import Page
from .singlefile import SingleFile

# from .multifile_disk import MultiFileDisk
from .disk import Disk

__all__ = ("getDisk", "Page")


def getDisk(name: str, data_dpath, config: DiskConfig) -> Disk:
    if config.typ == "singlefile":
        return SingleFile(name, data_dpath, config)
    # elif config.typ == "multifile":
    #     return MultiFileDisk(name, data_dpath, config)
    else:
        raise Exception(f"unknown meta: disk.typ: {config.typ}")
