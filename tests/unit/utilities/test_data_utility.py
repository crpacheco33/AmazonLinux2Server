import pytest

from tests.test_constants import TestConstants


@pytest.mark.utility
def test_delta_calculates_delta_correctly(data_utility):
    a, b = 100, 50

    expected = 100
    actual = data_utility.delta(a, b)

    assert expected == actual

    a, b = 50, 100

    expected = -50
    actual = data_utility.delta(a, b)

    assert expected == actual


@pytest.mark.utility
def test_delta_returns_100_when_dividing_number_by_zero(data_utility):
    a, b = 10, 0

    expected = 100
    actual = data_utility.delta(a, b)

    assert expected == actual


@pytest.mark.utility
def test_delta_returns_0_when_dividing_zero_by_zero(data_utility):
    a, b = 0, 0

    expected = 0
    actual = data_utility.delta(a, b)

    assert expected == actual


@pytest.mark.utility
def test_to_camel_case_returns_camel_cased_string(data_utility):
    value = 'get_visibly'

    expected = 'getVisibly'
    actual = data_utility.to_camel_case(value)

    assert expected == actual

    value = 'get_visibly_com'

    expected = 'getVisiblyCom'
    actual = data_utility.to_camel_case(value)

    assert expected == actual


@pytest.mark.utility
def test_to_objectives_and_segments_returns_all_objectives_and_segments_if_none_provided(data_utility):
    actual_objectives, actual_segments = data_utility.to_objectives_and_segments(None)

    expected = 9
    actual = len(actual_objectives)

    assert expected == actual

    expected = 3
    actual = len(actual_segments)

    assert expected == actual

    expected = ['AM', 'BP', 'CQ', 'DC', 'UZ', 'XM', 'CM', 'IM', 'RM']
    
    assert expected == actual_objectives

    expected = ['AW', 'CS', 'CV']

    assert expected == actual_segments

@pytest.mark.utility
def test_to_objectives_and_segments_groups_items_by_objective_and_segment(data_utility):
    items = [
        'AW', 'CS', 'CV', 'AM', 'BP', 'CQ', 'DC', 'UZ', 'XM', 'CM', 'IM', 'RM',
    ]

    actual_objectives, actual_segments = data_utility.to_objectives_and_segments(items)

    expected = 9
    actual = len(actual_objectives)

    assert expected == actual

    expected = 3
    actual = len(actual_segments)

    assert expected == actual

    expected = ['AM', 'BP', 'CQ', 'DC', 'UZ', 'XM', 'CM', 'IM', 'RM']
    
    assert sorted(expected) == sorted(actual_objectives)

    expected = ['AW', 'CS', 'CV']

    assert sorted(expected) == sorted(actual_segments)


@pytest.mark.utility
def test_to_camel_case_returns_value_without_underscores(data_utility):
    value = 'get'

    expected = 'get'
    actual = data_utility.to_camel_case(value)

    assert expected == actual
