from datetime import datetime

import pytest

from server.core.constants import Constants
from tests.test_constants import TestConstants


@pytest.mark.utility
def test_is_full_week_returns_true_when_dates_align_with_week(date_utility):
    start_date = datetime(3000, 1, 6)  # Monday
    end_date = datetime(3000, 1, 12)  # Sunday

    expected = True
    actual = date_utility.is_full_week(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_week_returns_false_when_dates_do_not_align_with_week(date_utility):
    start_date = datetime(3000, 1, 7)  # Tuesday
    end_date = datetime(3000, 4, 13)  # Monday

    expected = False
    actual = date_utility.is_full_week(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_month_returns_true_when_dates_align_with_month(date_utility):
    start_date = datetime(3000, 1, 1)
    end_date = datetime(3000, 1, 31)

    expected = True
    actual = date_utility.is_full_month(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_month_returns_false_when_dates_do_not_align_with_month(date_utility):
    start_date = datetime(3000, 4, 1)
    end_date = datetime(3000, 4, 15)

    expected = False
    actual = date_utility.is_full_month(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_quarter_returns_true_when_dates_align_with_quarter(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3000, 3, 31).date()

    expected = True
    actual = date_utility.is_full_quarter(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_quarter_returns_false_when_dates_do_not_align_with_quarter(date_utility):
    start_date = datetime(3000, 4, 1)
    end_date = datetime(3000, 5, 31)

    expected = False
    actual = date_utility.is_full_quarter(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_year_returns_true_when_dates_align_with_year(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3000, 12, 31).date()

    expected = True
    actual = date_utility.is_full_year(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_is_full_year_returns_false_when_dates_do_not_align_with_year(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3001, 12, 31).date()

    expected = False
    actual = date_utility.is_full_year(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_previous_period_returns_previous_week(date_utility):
    start_date = datetime(3000, 1, 6)
    end_date = datetime(3000, 1, 12)

    expected = {
        'previous_period_start_date': datetime(2999, 12, 30).date(),
        'previous_period_end_date': datetime(3000, 1, 5).date(),
    }
    actual = date_utility.previous_period(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_previous_period_returns_previous_month(date_utility):
    start_date = datetime(3000, 1, 1)
    end_date = datetime(3000, 1, 31)

    expected = {
        'previous_period_start_date': datetime(2999, 12, 1).date(),
        'previous_period_end_date': datetime(2999, 12, 31).date(),
    }
    actual = date_utility.previous_period(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_previous_period_returns_previous_quarter(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3000, 3, 31).date()

    expected = {
        'previous_period_start_date': datetime(2999, 10, 1).date(),
        'previous_period_end_date': datetime(2999, 12, 31).date(),
    }
    actual = date_utility.previous_period(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_previous_period_returns_previous_year(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3000, 12, 31).date()

    expected = {
        'previous_period_start_date': datetime(2999, 1, 1).date(),
        'previous_period_end_date': datetime(2999, 12, 31).date(),
    }
    actual = date_utility.previous_period(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_previous_period_returns_arbitary_previous_period(date_utility):
    start_date = datetime(3000, 1, 1).date()
    end_date = datetime(3000, 2, 14).date()

    expected = {
        'previous_period_start_date': datetime(2999, 11, 17).date(),
        'previous_period_end_date': datetime(2999, 12, 31).date(),
    }
    actual = date_utility.previous_period(start_date, end_date)

    assert expected == actual


@pytest.mark.utility
def test_timestamp_to_date_returns_date(date_utility):
    timestamp = 32503708800  # seconds

    expected = datetime(3000, 1, 1, 8, 0, 0).date()
    actual = date_utility.timestamp_to_date(timestamp)

    assert expected == actual

@pytest.mark.utility
def test_to_string_returns_formatted_date(date_utility):
    date = datetime(3000, 1, 1).date()

    expected = '3000-01-01'
    actual = date_utility.to_string(date)

    assert expected == actual

    expected = '3000-01-01'
    actual = date_utility.to_string(
        date,
        Constants.DATE_FORMAT_YYYY_MM_DD,
    )

    assert expected == actual

    expected = '30000101'
    actual = date_utility.to_string(
        date,
        Constants.YYYYMMDD_DATE_FORMAT,
    )

    assert expected == actual

