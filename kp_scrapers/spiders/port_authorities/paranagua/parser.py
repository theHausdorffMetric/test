from kp_scrapers.lib.parser import may_strip


def remove_unwanted_space_characters(lst):
    return (may_strip(item) for item in lst)
