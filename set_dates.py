from datetime import date, timedelta, datetime
import pandas as pd

def set_dates():
    first_date_str = "2025-01-01"
    first_date = pd.to_datetime(first_date_str)
    month_of_first_date = int(pd.to_datetime(first_date).strftime("%m"))
    if month_of_first_date < 12:
        date_of_next_month = date(first_date.year, first_date.month + 1, 1)
    else:
        date_of_next_month = date(first_date.year + 1, 1, 1)
    last_date = (date_of_next_month - timedelta(days=1) ).strftime("%Y-%m-%d")
    return first_date_str, last_date