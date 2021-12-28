from datetime import (
    datetime,
    timedelta,
)

import calendar

from datequarter import DateQuarter

from server.core.constants import Constants


class DateUtility:

    def is_full_week(self, start_date, end_date):
        start_of_week = start_date - timedelta(days=start_date.weekday())
        end_of_week = start_of_week + timedelta(days=6)

        is_start_of_week = start_of_week == start_date
        is_end_of_week = end_of_week == end_date

        return is_start_of_week and is_end_of_week
    
    def is_full_month(self, start_date, end_date):
        if start_date.month != end_date.month:
            return False
        
        difference_in_days = (end_date - start_date).days + 1
        days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]

        return difference_in_days == days_in_month

    def is_full_quarter(self, start_date, end_date):
        is_start_of_quarter = DateQuarter(
            start_date.year,
            start_date.month,
        ).start_date() == start_date
        
        is_end_of_quarter = DateQuarter(
            end_date.year,
            start_date.month,
        ).end_date() == end_date
        
        return is_start_of_quarter and is_end_of_quarter
    
    def is_full_year(self, start_date, end_date):
        is_start_of_year = datetime(
            start_date.year,
            1,
            1,
        ).date() == start_date

        is_end_of_year = datetime(
            start_date.year,
            12,
            31,
        ).date() == end_date

        return is_start_of_year and is_end_of_year

    def previous_period(self, start_date, end_date):
        def months(start_date, end_date):
            difference =  end_date.month - start_date.month + 12 * (end_date.year - start_date.year)
            return difference
        
        difference_in_days = (end_date - start_date).days
        difference_in_months = months(start_date, end_date)

        if self.is_full_week(start_date, end_date):
            previous_start_date = start_date - timedelta(days=7)
            previous_end_date = end_date - timedelta(days=7)
        elif self.is_full_month(start_date, end_date):
            if start_date.month == 1:
                previous_start_year = start_date.year - 1
                previous_start_date = datetime(
                    previous_start_year, 12, 1,
                )
            else:
                previous_start_year = start_date.year
                previous_start_date = datetime(
                    previous_start_year, start_date.month - 1, 1,
                )
            
            previous_end_date = start_date - timedelta(days=1)
        elif self.is_full_quarter(start_date, end_date):
            previous_quarter = DateQuarter(
                start_date.year,
                start_date.month,
            ) - 1
            previous_start_date = previous_quarter.start_date()
            previous_end_date = previous_quarter.end_date()

            return {
                Constants.PREVIOUS_PERIOD_START_DATE: previous_start_date,
                Constants.PREVIOUS_PERIOD_END_DATE: previous_end_date,
            }
        elif self.is_full_year(start_date, end_date):
            previous_start_date = datetime(
                start_date.year - 1,
                start_date.month,
                start_date.day,
            )
            previous_end_date = datetime(
                end_date.year - 1,
                end_date.month,
                end_date.day,
            )
        else:
            previous_end_date = start_date - timedelta(days=1)
            previous_start_date = previous_end_date - timedelta(days=difference_in_days)
            return {
                Constants.PREVIOUS_PERIOD_START_DATE: previous_start_date,
                Constants.PREVIOUS_PERIOD_END_DATE: previous_end_date,
            }

        try:
            return {
                Constants.PREVIOUS_PERIOD_START_DATE: previous_start_date.date(),
                Constants.PREVIOUS_PERIOD_END_DATE: previous_end_date.date(),
            }
        except AttributeError:
            return {
                Constants.PREVIOUS_PERIOD_START_DATE: previous_start_date,
                Constants.PREVIOUS_PERIOD_END_DATE: previous_end_date,
            }

    def to_string(self, date_string, date_format=Constants.ISO_DATE_FORMAT):
        return datetime.strftime(date_string, date_format)
    
    def timestamp_to_date(self, timestamp):
        return datetime.fromtimestamp(timestamp).date()
    
    def to_date(self, date, date_format=Constants.ISO_DATE_FORMAT):
        return datetime.strptime(date, date_format)
    
    def to_es_dates(self, start_date, end_date):
        return (
            self.to_date(start_date, date_format=Constants.YYYYMMDD_DATE_FORMAT),
            self.to_date(end_date, date_format=Constants.YYYYMMDD_DATE_FORMAT),
        )
        
    def to_sa_dates(self, start_date, end_date):
        return (
            self.to_date(start_date, date_format=Constants.YYYYMMDD_DATE_FORMAT),
            self.to_date(end_date, date_format=Constants.YYYYMMDD_DATE_FORMAT),
        )

    def to_string(self, date, date_format=Constants.ISO_DATE_FORMAT):
        return date.strftime(date_format)