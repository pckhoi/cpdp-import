import os
import json

import numpy as np
import pandas as pd

from lib.clean import ensure_int

with open(os.path.join(os.path.dirname(__file__), "../ref/beat.json"), 'r') as f:
    BEAT_DICT = json.loads(f.read())


with open(os.path.join(os.path.dirname(__file__), "../ref/category.json"), 'r') as f:
    CATEGORY_DICT = json.loads(f.read())


with open(os.path.join(os.path.dirname(__file__), "../ref/units.json"), 'r') as f:
    UNIT_DICT = json.loads(f.read())


def lookup_beat(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].fillna('').astype(str)\
            .str.zfill(4).map(lambda x: np.NaN if x == '0000' else BEAT_DICT[x])
    return df


def lookup_unit(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].fillna('').astype(str)\
            .str.zfill(3).map(lambda x: np.NaN if x == '000' else UNIT_DICT[x])
    return df


def lookup_category(df, cols):
    for col in cols:
        df.loc[:, col] = df[col].map(lambda x: CATEGORY_DICT[x])
    return df


def load_officers():
    df = pd.read_csv(os.path.join(
        os.path.dirname(__file__), "../ref/officer.csv"))
    for col in ['last_name', 'first_name', 'middle_initial', 'gender', 'race']:
        df.loc[:, col] = df[col].fillna('').str.lower()
    for col in ['resignation_date', 'appointed_date']:
        df.loc[:, col] = pd.to_datetime(df[col], format='%Y-%m-%d')
    return df\
        .pipe(ensure_int, ['birth_year'])
