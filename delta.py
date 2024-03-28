from pandas import read_pickle, DataFrame

# load two dataframes from file (pickle)
# systematically compare them

import logging

def load_dataframe_from_file(file_path: str, compression: str) -> DataFrame:

    print(f'loading dataframe from pkl file at {file_path} ...')

    df = read_pickle(
        filepath_or_buffer=file_path,
        compression=compression
    )

    print(f'... loaded dataframe from pkl file at {file_path}')

    return df


# pandas.DataFrame.compare

def compare(alpha: DataFrame, beta: DataFrame) -> bool:
    differences = []

    # row count

    alpha_row_count = len(alpha.index)
    beta_row_count = len(beta.index)

    if alpha_row_count != beta_row_count:
        differences.append(f'row counts do not match: alpha = {alpha_row_count}, beta = {beta_row_count}')

    # column count

    alpha_col_count = len(alpha.columns.to_list())
    beta_col_count = len(beta.columns.to_list())

    if alpha_col_count != beta_col_count:
        differences.append(f'column counts do not match: alpha = {alpha_col_count}, beta = {beta_col_count}')

    # column name
        
    alpha_cols = alpha.columns.to_list()
    beta_cols = beta.columns.to_list()

    column_union = set(alpha_cols + beta_cols)
    column_intersection = [col for col in column_union if col in alpha_cols and col in beta_cols]
    column_disjoint = [col for col in column_union if col not in column_intersection]

    if len(column_disjoint) > 0:
        differences.append(f'disjoint columns: {"".join(column_disjoint)}')

    for i in range(alpha_col_count):
        if alpha_cols[i] != beta_cols[i]:
            differences.append(f'column order does not match: {i} alpha {alpha_cols[i]}, beta {beta_cols[i]}')

    # column types

    for i in range(alpha_col_count):
        alpha_dtype = alpha.dtypes.iloc[i]
        beta_dtype = beta.dtypes.iloc[i]
        if alpha_dtype != beta_dtype:
            alpha_col_name = alpha_cols[i]
            beta_col_name = beta_cols[i]
            differences.append(f'column types do not match: alpha {alpha_col_name} {alpha_dtype}, beta {beta_col_name} {beta_dtype}')

    # --------------------------------------

    col_count = alpha_col_count

    for row_idx in range(1):
        for col_idx in range(col_count):
            col = alpha_cols[col_idx]
            alpha_val = alpha[col].iloc[row_idx]
            beta_val = beta[col].iloc[row_idx]
            #print(col, alpha_val, beta_val)
            if alpha_val != beta_val:
                differences.append(f'row {row_idx}, col {col}: alpha {alpha_val}, beta {beta_val}')

    #     # values
            
    #     for column in column_union: 
    #         for row_idx in range(alpha_row_count):
    #             if alpha[column][row_idx] != beta[column][row_idx]:
    #                 alpha_val = alpha[column][row_idx]
    #                 beta_val = beta[column][row_idx]

    #                 print(f'value difference: column {column} row {row_idx}: alpha {alpha_val}, beta {beta_val}')
         
    identical = len(differences) == 0

    if not identical:
        print('DIFFERENCES:')
        [print(difference) for difference in differences]

    return identical

local_dev_file_path = '/Users/david.barkhuizen/development/lumos/mount/celery-worker-heavy/etl_input_set.pkl'
local_dev = load_dataframe_from_file(local_dev_file_path, compression='zip')

on_prem_file_path = '/Users/david.barkhuizen/code/delta-df/in/on_prem.pkl'
on_prem = load_dataframe_from_file(on_prem_file_path, compression='gzip')

identical = compare(on_prem, local_dev)
if identical:
    print('data frames are identical')

#diff = on_prem.compare(local_dev, align_axis=1, keep_shape=False, keep_equal=False, result_names=('on-prem', 'local-dev'))
#print(diff)