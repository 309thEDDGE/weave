"""Wherein functionality concerning listing basket jsons is contained."""
import os

from fsspec import AbstractFileSystem

def _get_list_of_basket_jsons(
    root_dir: str, file_system: AbstractFileSystem
) -> list[str]:
    """Return a list of basket manifest paths in the given root dir.

    Parameters:
    -----------
    root_dir: str
        Path to search for basket manifests--doesn't have to be the pantry root
    file_system: fsspec object
        The file system to search in.

    Returns:
    ----------
    A list of paths to basket_manifest.json files found under the given dir
    """
    # In some instances, root_dir may be the empty str and should be left as is
    root_dir = os.path.normpath(root_dir) if root_dir != '' else ''
    # On Windows find() returns with forward slashes, the root dir must match.
    root_dir = root_dir.replace(os.sep, '/')
    manifest_paths = []
    for path in file_system.find(root_dir):
        if path.endswith("basket_manifest.json"):
            # The find method returns absolute paths which need to be trimmed
            # to start from the root_dir instead of the full path.
            # This is done to ensure returned paths are relative to the pantry.
            manifest_paths.append(path[path.index(root_dir):])
    return manifest_paths
