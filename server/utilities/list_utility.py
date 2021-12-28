def partition_list(list, n):
    """
    Divide a given list into multiple sublist that contains no more than
    n partitions.
    """
    return [(list[i: i + n]) for i in range(0, len(list), n)]
