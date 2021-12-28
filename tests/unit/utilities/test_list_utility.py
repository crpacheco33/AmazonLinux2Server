import pytest

from server.utilities.list_utility import partition_list


@pytest.mark.utility
def test_partition_list_partitions_small_list_correctly():
    data = [0 for _ in range(0, 1)]

    partitioned_list = partition_list(
        data,
        1024,
    )

    expected = 1
    actual = len(partitioned_list)

    assert expected == actual

    expected = 1
    actual = len(partitioned_list[-1])

    assert expected == actual


@pytest.mark.utility
def test_partition_list_partitions_large_list_correctly():
    data = [0 for _ in range(0, 2560)]

    partitioned_list = partition_list(
        data,
        1024,
    )

    expected = 3
    actual = len(partitioned_list)

    assert expected == actual

    expected = 512
    actual = len(partitioned_list[-1])

    assert expected == actual
