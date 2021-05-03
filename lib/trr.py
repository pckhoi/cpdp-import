import pandas as pd

from lib.clean import (
    clean_column_names, clean_dates, clean_genders, clean_races, ensure_int, fill_na_with_none, ensure_bool,
    ensure_upper_case, ensure_title_case, clean_datetimes
)
from lib.refs import lookup_unit


def clean_direction(df):
    dir_dict = {
        'W': 'West',
        'E': 'East',
        'S': 'South',
        'N': 'North'
    }
    df.loc[:, 'direction'] = df.direction.map(lambda x: dir_dict[x])
    return df


def clean_trr_datetime(df, cols):
    for col in cols:
        df.loc[:, col] = pd.to_datetime(
            df[col], format='%Y-%b-%d %H%M').dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


def clean_trr(df):
    return df\
        .pipe(clean_column_names)\
        .drop(columns=[
            'rd_no', 'cr_no_obtained', 'subject_cb_no', 'subject_ir_no', 'event_no', 'polast', 'pofirst',
            'pomi', 'pogndr', 'porace', 'poyrofbirth', 'appointed_date', 'currrank', 'currunit', 'currbeatassg',
            'rank', 'trr_created', 'policycompliance'
        ])\
        .rename(columns={
            'trr_report_id': 'id',
            'blk': 'block',
            'stn': 'street',
            'dir': 'direction',
            'loc': 'location',
            'datetime': 'trr_datetime',
            'notify_oemc': 'notify_OEMC',
            'notify_dist_sergeant': 'notify_district_sergeant',
            'notify_op_command': 'notify_OP_command',
            'notify_det_div': 'notify_DET_division',
            'unitassg': 'officer_unit_id',
            'unitdetail': 'officer_unit_detail_id',
            'assgnbeat': 'officer_assigned_beat',
            'dutystatus': 'officer_on_duty',
            'poinjured': 'officer_injured',
            'member_in_uniform': 'officer_in_uniform',
            'subgndr': 'subject_gender',
            'subrace': 'subject_race',
            'subyeardob': 'subject_birth_year'
        })\
        .pipe(clean_direction)\
        .pipe(clean_trr_datetime, ['trr_datetime'])\
        .pipe(clean_races, ['subject_race'])\
        .pipe(ensure_title_case, 'indoor_or_outdoor', ['Indoor', 'Outdoor'])\
        .pipe(ensure_upper_case, 'lighting_condition', [
            'DAYLIGHT', 'GOOD ARTIFICIAL', 'DUSK', 'NIGHT', 'POOR ARTIFICIAL', 'DAWN', 'ARTIFICIAL', 'DARKNESS'])\
        .pipe(ensure_upper_case, 'weather_condition', [
            'OTHER', 'CLEAR', 'SNOW', 'RAIN', 'SLEET/HAIL', 'SEVERE CROSS WIND', 'FOG/SMOKE/HAZE'])\
        .pipe(ensure_upper_case, 'party_fired_first', ['MEMBER', 'OTHER', 'OFFENDER'])\
        .pipe(ensure_upper_case, 'subject_race')\
        .pipe(ensure_upper_case, 'officer_assigned_beat')\
        .pipe(ensure_bool, [
            'notify_OEMC', 'notify_district_sergeant', 'notify_OP_command', 'notify_DET_division',
            'officer_on_duty', 'officer_injured', 'officer_in_uniform', 'subject_armed', 'subject_injured',
            'subject_alleged_injury'])\
        .pipe(lookup_unit, ['officer_unit_id', 'officer_unit_detail_id'])\
        .pipe(ensure_int, [
            'number_of_weapons_discharged', 'officer_unit_id', 'officer_unit_detail_id', 'subject_birth_year'])\
        .pipe(clean_genders, ['subject_gender'])\
        .pipe(fill_na_with_none)


def clean_action_response(df):
    return clean_column_names(df)\
        .rename(columns={
            'trr_report_id': 'trr_id'
        })\
        .pipe(ensure_title_case, 'person', ['Member Action', 'Subject Action'])\
        .pipe(ensure_title_case, 'resistance_type', [
            'Active Resister', 'Passive Resister', 'Assailant Battery', 'Assailant Assault/Battery',
            'Assailant Assault', 'Assailant Deadly Force'
        ])\
        .pipe(ensure_upper_case, 'action')


def clean_charge(df):
    return clean_column_names(df)\
        .rename(columns={
            'trr_report_id': 'trr_id',
            'descr': 'description'
        })\
        .drop(columns=['subject_cb_no', 'rd_no'])\
        .pipe(ensure_upper_case, 'description')


def clean_subject_weapon(df):
    return clean_column_names(df)\
        .rename(columns={
            'trr_report_id': 'trr_id',
        })\
        .pipe(ensure_upper_case, 'weapon_type')\
        .pipe(ensure_upper_case, 'weapon_description')


def clean_status(df):
    return clean_column_names(df)\
        .rename(columns={
            'trr_report_id': 'trr_id',
            'statdatetime': 'status_datetime',
            'first_nme': 'last_name',
            'last_nme': 'first_name',
            'mi': 'middle_initial',
            'sex': 'gender',
            'memberrace': 'race',
            'yearofbirth': 'birth_year'
        })\
        .drop(columns=['trr_status', 'rank', 'unit_at_incident'])\
        .pipe(clean_datetimes, ['status_datetime'])\
        .pipe(clean_races, ['race'])\
        .pipe(clean_dates, ['appointed_date'], True)


def clean_weapon_discharge(df):
    return clean_column_names(df)\
        .rename(columns={
            'trr_report_id': 'trr_id',
        })\
        .pipe(ensure_upper_case, 'weapon_type')\
        .pipe(ensure_upper_case, 'firearm_make')\
        .pipe(ensure_upper_case, 'firearm_model')\
        .pipe(ensure_upper_case, 'handgun_worn_type')\
        .pipe(ensure_upper_case, 'handgun_drawn_type')\
        .pipe(ensure_upper_case, 'method_used_to_reload')\
        .pipe(ensure_upper_case, 'protective_cover_used')\
        .pipe(ensure_upper_case, 'discharge_distance')\
        .pipe(ensure_upper_case, 'object_struck_of_discharge')\
        .pipe(ensure_upper_case, 'discharge_position')\
        .pipe(ensure_int, ['total_number_of_shots', 'number_of_catdridge_reloaded'])\
        .pipe(ensure_bool, ['firearm_reloaded', 'sight_used'])
