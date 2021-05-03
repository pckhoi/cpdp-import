import re
import os
import json
import datetime

import pandas as pd
import numpy as np


def ensure_int(df, cols):
    for col in cols:
        s = df[col]
        df.loc[:, col] = s.where(s.notna(), None)\
            .where(s.isna(), s.fillna(0).astype(int))
    return df


def ensure_bool(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].fillna('f').str.lower()\
            .str.replace(r'^no?$', 'F', regex=True)\
            .str.replace(r'^y(es)?$', 'T', regex=True)\
            .str.upper()
    return df


def ensure_title_case(df, col, choices=None):
    df.loc[:, col] = df[col].fillna('').str.strip().str.title()
    df.loc[:, col] = df[col].where(df[col] != '', None)
    if choices is not None and not df.loc[df[col].notna(), col].isin(choices).all():
        raise ValueError('unrecognized values for column "%s": %s' % (
            col, set(df[col].unique()) - set(choices)
        ))
    return df


def ensure_upper_case(df, col, choices=None):
    df.loc[:, col] = df[col].fillna('').str.strip().str.upper()
    df.loc[:, col] = df[col].where(df[col] != '', None)
    if choices is not None and not df.loc[df[col].notna(), col].isin(choices).all():
        raise ValueError('unrecognized values for column "%s": %s' % (
            col, set(df[col].unique()) - set(choices)
        ))
    return df


def fill_na_with_none(df):
    return df.where(df.notna(), None)


def clean_column_names(df):
    """
    Remove unnamed columns and convert to snake case
    """
    df = df[[col for col in df.columns if not col.startswith("Unnamed:")]]
    df.columns = [
        re.sub(r"[\s\W]+", "_", col.strip()).lower().strip("_")
        for col in df.columns]
    return df


def float_to_int_str(df, cols, cast_as_str=False):
    """
    Turn float values in column into strings without trailing ".0"
    e.g. [1973.0, np.nan, "abc"] => ["1973", "", "abc"]
    """
    cols_set = set(df.columns)
    for col in cols:
        if col not in cols_set:
            continue
        if df[col].dtype == np.float64:
            df.loc[:, col] = df[col].fillna(0).astype(
                "int64").astype(str).str.replace(r"^0$", "", regex=True)
        elif df[col].dtype == np.object:
            idx = df[col].map(lambda v: type(v) == float)
            df.loc[idx, col] = df.loc[idx, col].fillna(0).astype(
                "int64").astype(str).str.replace(r"^0$", "", regex=True)
            if cast_as_str:
                df.loc[~idx, col] = df.loc[~idx, col].astype(str)
        elif cast_as_str:
            df.loc[:, col] = df[col].astype(str)
    return df


mdy_date_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")
mdy_date_pattern_2 = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")
dmy_date_pattern = re.compile(r'^\d{1,2}-\w{3}-\d{2}$')
ymd_date_pattern = re.compile(r'^\d{4}-\w{3}-\d{2}$')
year_pattern = re.compile(r"^(19|20)\d{2}$")
year_month_pattern = re.compile(r"^(19|20)\d{4}$")
month_day_pattern = re.compile(r"^[A-Z][a-z]{2}-\d{1,2}$")
time_pattern_1 = re.compile(r"\d{2}:\d{2}\.\d$")


def clean_date(val):
    """
    Try parsing date with various patterns and return a tuple of (year, month, day)
    """
    if val == "" or pd.isnull(val):
        return "", "", ""
    m = mdy_date_pattern_2.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        return year, month, day
    m = mdy_date_pattern_1.match(val)
    if m is not None:
        [month, day, year] = val.split("/")
        if year[0] in ["1", "2", "0"]:
            year = "20" + year
        else:
            year = "19" + year
        return year, month, day
    m = dmy_date_pattern.match(val)
    if m is not None:
        [day, month, year] = val.split("-")
        month = str(datetime.datetime.strptime(month, '%b').month)
        if year[0] in ["1", "2", "0"]:
            year = "20" + year
        else:
            year = "19" + year
        return year, month, day
    m = ymd_date_pattern.match(val)
    if m is not None:
        [year, month, day] = val.split("-")
        month = str(datetime.datetime.strptime(month, '%b').month)
        return year, month, day
    m = year_month_pattern.match(val)
    if m is not None:
        return val[:4], val[4:], ""
    m = year_pattern.match(val)
    if m is not None:
        return val, "", ""
    m = month_day_pattern.match(val)
    if m is not None:
        dt = datetime.datetime.strptime(val, "%b-%d")
        return "", str(dt.month).zfill(2), str(dt.day).zfill(2)
    raise ValueError("unknown date format \"%s\"" % val)


def clean_dates(df, cols, as_date=False):
    for col in cols:
        dates = pd.DataFrame.from_records(
            df[col].str.strip().str.lower().str.replace(
                r'^redacted$', '', regex=True
            ).map(clean_date).tolist()
        )
        dates.columns = ['year', 'month', 'day']
        df.loc[:, col] = pd.to_datetime(dates)
        if not as_date:
            df.loc[:, col] = df[col].dt.strftime('%Y-%m-%d')
    return df


datetime_pattern_1 = re.compile(r"^\d{1,2}/\d{1,2}/\d{2,4}\s+\d{1,2}:\d{1,2}$")


def clean_datetime(val):
    if val == "" or pd.isnull(val):
        return "", "", "", "", ""
    m = datetime_pattern_1.match(val)
    if m is not None:
        [date, time] = re.split(r"\s+", val)
        [hour, minute] = time.split(":")
        year, month, day = clean_date(date)
        return year, month, day, hour, minute
    raise ValueError("unknown datetime format \"%s\"" % val)


def clean_datetimes(df, cols):
    for col in cols:
        dates = pd.DataFrame.from_records(
            df[col].str.strip().map(clean_datetime))
        dates.columns = ['year', 'month', 'day', 'hour', 'minute']
        df.loc[:, col] = pd.to_datetime(dates).dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


def clean_times(df, cols, format='%I:%M %p'):
    for col in cols:
        df.loc[:, col] = pd.to_datetime(
            df[col].str.replace(r'^(\d):', r'0\1', regex=True)
            .str.replace(r'^(\d{2})(\d{2})', r'\1:\2', regex=True),
            format=format
        ).dt.strftime('%H:%M:%S')
    return df


RACE_WHITELIST = set([
    "Black",
    "Hispanic",
    "White",
    "Asian/Pacific Islander",
    "Native American/Alaskan Native",
    "",
])


def clean_races(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].str.lower().fillna('').str.replace(r'^black.*', 'Black', regex=True)\
            .str.replace(r'^blk', 'Black', regex=True)\
            .str.replace(r'^.*hispanic.*$', 'Hispanic', regex=True)\
            .str.replace(r'^s?panish.*', 'Hispanic', regex=True)\
            .str.replace(r'^whi.*', 'White', regex=True)\
            .str.replace(r'^wwh', 'White', regex=True)\
            .str.replace(r'^\w$', '', regex=True)\
            .str.replace(r'^api', 'Asian/Pacific Islander', regex=True)\
            .str.replace(r'^.*asian.*$', 'Asian/Pacific Islander', regex=True)\
            .str.replace(r'^.*alaskan.*$', 'Native American/Alaskan Native', regex=True)\
            .str.replace(r'^unknown$', '', regex=True)
        if (~df[col].isin(RACE_WHITELIST)).any():
            all_races = set(df[col].tolist())
            raise ValueError('race value detected outside of whitelist:\n\t%s' % json.dumps(
                list(all_races - RACE_WHITELIST)))
    return df


GENDER_WHITELIST = set(['M', 'F', 'X', ''])


def clean_genders(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].fillna('').str.lower().str.strip()\
            .str.replace(r'^male.*', 'M', regex=True)\
            .str.replace(r'^female.*', 'F', regex=True).str.upper()
        if (~df[col].isin(GENDER_WHITELIST)).any():
            all_genders = set(df[col].tolist())
            raise ValueError('gender value detected outside of whitelist:\n\t%s' % json.dumps(
                list(all_genders - GENDER_WHITELIST)))
    return df


def clean_finding(df, col='final_finding'):
    finding_dict = {
        'unfounded': 'UN',
        'exonerated': 'EX',
        'not sustained': 'NS',
        'sustained': 'SU',
        'no cooperation': 'NC',
        'no affidavit': 'NA',
        'discharged': 'DS',
        'unknown': 'ZZ',
    }
    df.loc[:, col] = df[col].str.lower().map(
        lambda x: '' if pd.isnull(x) else finding_dict[x])
    return df


def clean_table(df):
    df = clean_column_names(df)\
        .dropna(axis=1, how='all')\
        .rename(columns={
            'log_no': 'crid'
        })
    df.loc[:, 'crid'] = df.crid.astype(str).str.replace('-', '', regex=False)
    return df


def clean_investigators(df):
    df = clean_table(df)\
        .rename(columns={
            'current_unit_assigned': 'current_unit',
            'unit_assigned_at_complaint': 'unit_at_complaint'
        })\
        .pipe(float_to_int_str, ["current_unit", 'unit_at_complaint', 'birth_year', 'star_no'])\
        .pipe(clean_races, ['race'])\
        .pipe(clean_dates, ['appointed_date', 'assign_datetime'])\
        .pipe(clean_unit, ['current_unit', 'unit_at_complaint'])\
        .pipe(clean_genders, ['gender'])
    df.loc[:, 'first_name'] = df.first_name.str.title()
    df.loc[:, 'last_name'] = df.last_name.str.title()
    return df
