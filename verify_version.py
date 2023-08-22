import sys


def verify_version(old_vers: str, new_vers: str) -> bool:
    """Verify the new version is valid.

    Parameters
    ----------
    old_vers: string
        Existing version number, likely from main.
    new_vers: string
        New version number from current branch.

    Outputs
    ----------
    validity: bool
        Validity of the new version number.
    """
    old_vers = [int(v) for v in old_vers.split('.')]
    valids = [
        [old_vers[0]+1, 0, 0],
        [old_vers[0], old_vers[1]+1, 0],
        [old_vers[0], old_vers[1], old_vers[2]+1],
    ]

    valids = [('.').join([str(v) for v in valid]) for valid in valids]

    if new_vers in valids:
        return True
    print(f"New version not valid. Valid version increments: {valids}")
    return False


if __name__=="__main__":
    assert len(sys.argv) == 3
    old_version = sys.argv[1]
    new_version = sys.argv[2]
    print(f"Old Version: {old_version}")
    print(f"New Version: {new_version}")

    valid = verify_version(old_version, new_version)
    sys.exit(not valid)