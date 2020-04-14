def naive_list_hash(row, *column_idx):
    """Compute a naive hash of a list by selected elements.

    "Hash" is computed by naively concatenating selected elements,
    with a comma delimiter.

    Args:
        row (List[str]):
        *column_idx: 0-based index of elements to be hashed on

    Returns:
        str: naive hash

    Examples:
        >>> naive_list_hash(['foo', 'bar', 'baz'], 1)
        'bar'
        >>> naive_list_hash(['foo', 'bar', 'baz'], 1, 2)
        'barbaz'
        >>> naive_list_hash(['foo', 'bar', 'baz'], 2, 1)
        'barbaz'

    """
    row_hash = ''
    for idx in sorted(column_idx):
        row_hash += row[idx]

    return row_hash
