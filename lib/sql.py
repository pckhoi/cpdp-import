import pandas as pd


def insert_stmts(table_name, df, include_timestamp=True):
    col_names = ', '.join([
        col if col.lower() == col else '"%s"' % col
        for col in df.columns
    ])
    ts_vals = []
    if include_timestamp:
        col_names += ', created_at, updated_at'
        ts_vals = ['current_timestamp'] * 2
    if 'id' not in df.columns:
        col_names += ', id'
        ts_vals += ['(select max(id) + 1 from %s)' % table_name]
    return '\n'.join([
        'INSERT INTO %s (%s) VALUES (%s);' % (
            table_name,
            col_names,
            ', '.join([
                'NULL' if pd.isnull(row[col]) else (
                    "'%s'" % row[col] if type(row[col]) == str else str(row[col]))
                for col in df.columns
            ]+ts_vals)
        ) for _, row in df.iterrows()])
