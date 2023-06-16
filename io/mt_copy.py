from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from shutil import copy2


def mt_copy2(src, dst, *, follow_symlinks=True, max_workers=None):
    if src and dst:
        if not isinstance(src, list):
            src = [src]
        if not isinstance(dst, list):
            dst = [dst]
        if len(src) == len(dst):
            with ThreadPoolExecutor(max_workers) as executor:
                futures = [executor.submit(copy2, src[i], dst[i], follow_symlinks=follow_symlinks) for i in range(len(src))]
        elif len(dst) == 1 and Path(dst[0]).is_dir():
            with ThreadPoolExecutor(max_workers) as executor:
                futures = [executor.submit(copy2, src[i], dst[0], follow_symlinks=follow_symlinks) for i in range(len(src))]
        elif len(src) == 1 and Path(src[0]).is_file():
            with ThreadPoolExecutor(max_workers) as executor:
                futures = [executor.submit(copy2, src[0], dst[i], follow_symlinks=follow_symlinks) for i in range(len(dst))]
        else:
            raise ValueError("src and dst must have the same length or dst must be a directory or src must a single file.")
    else:
        raise ValueError("src and dst must be non-empty lists.")


