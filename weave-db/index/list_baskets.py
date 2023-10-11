"""Wherein functionality concerning listing basket jsons is contained."""

def _get_list_of_basket_jsons(root_dir, file_system):
    return [x for x in file_system.find(root_dir)
            if x.endswith("basket_manifest.json")]
