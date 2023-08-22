import sys

def verify_version(old_vers, new_vers):
    old_vers = [int(v) for v in old_vers.split('.')]
    valids = [
        [old_vers[0]+1, 0, 0],
        [old_vers[0], old_vers[1]+1, 0],
        [old_vers[0], old_vers[1], old_vers[2]+1],
    ]

    for i, valid in enumerate(valids):
        valids[i] = ('.').join([str(v) for v in valids[i]])

    if new_vers in valids:
        return True
    print(f"New version not valid. Valid version increments: {valids}")
    return False

if __name__=="__main__":
    old_version = sys.argv[1]
    new_version = sys.argv[2]
    print(f"Old Version: {old_version}")
    print(f"New Version: {new_version}")

    verify_version(old_version, new_version)