# load two dataframes from file (pickle)
# systematically compare them

from pandas import NaT, read_pickle, DataFrame
#import logging
import pandas
import sys
from collections.abc import Callable

def load_dataframe_from_file(file_path: str, compression: str) -> DataFrame:

    print(f'loading dataframe from pkl file at {file_path} ...')

    df = read_pickle(
        filepath_or_buffer=file_path,
        compression=compression
    )

    print(f'... loaded dataframe from pkl file at {file_path}')

    return df


# pandas.DataFrame.compare

def compare(alpha_label: str, alpha: DataFrame, beta_label: str, beta: DataFrame) -> bool:
    differences = []

    # row count

    alpha_row_count = len(alpha.index)
    beta_row_count = len(beta.index)

    if alpha_row_count != beta_row_count:
        differences.append(f'row counts do not match: {alpha_label} = {alpha_row_count}, {beta_label} = {beta_row_count}')

    # column count

    alpha_col_count = len(alpha.columns.to_list())
    beta_col_count = len(beta.columns.to_list())

    if alpha_col_count != beta_col_count:
        differences.append(f'column counts do not match: {alpha_label} = {alpha_col_count}, {beta_label} = {beta_col_count}')

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
            differences.append(f'column order does not match: {i} {alpha_label} {alpha_cols[i]}, {beta_label} {beta_cols[i]}')

    # column types

    for i in range(alpha_col_count):
        alpha_dtype = alpha.dtypes.iloc[i]
        beta_dtype = beta.dtypes.iloc[i]
        alpha_col_name = alpha_cols[i]
        beta_col_name = beta_cols[i]
        if alpha_dtype != beta_dtype:
            differences.append(f'column types do not match: {alpha_label} {alpha_col_name} {alpha_dtype}, {beta_label} {beta_col_name} {beta_dtype}')
        #print(f'dtype for {alpha_col_name}: {alpha_dtype}')

    # --------------------------------------
                    
    def value_delta():
        value_diff_count = 0

        cols_with_val_diffs = []

        for row_idx in range(alpha_row_count):

            # only log differences from the first differing row
            log_difference = value_diff_count < 1
            row_has_difference = False 

            for col_idx in range(alpha_col_count):
                col = alpha_cols[col_idx]
                alpha_val = alpha[col].iloc[row_idx]
                beta_val = beta[col].iloc[row_idx]
                if alpha_val != beta_val:
                    if not pandas.isnull(alpha_val) and not pandas.isnull(beta_val):
                        row_has_difference = True
                        value_diff_count = value_diff_count + 1
                        if col not in cols_with_val_diffs: cols_with_val_diffs.append(col)
                        
                        if log_difference:
                            text = f'value difference in row {row_idx} column {col}: {alpha_label} ({type(alpha_val)}) : {alpha_val} compare {beta_label} ({type(beta_val)}) : {beta_val}'
                            differences.append(text)

            if row_has_difference and log_difference:
                print('=-'*40)
                print(f'{alpha_label}')
                print()
                print(f'{alpha.iloc[row_idx]}')

                print('=-'*40)
                print(f'{beta_label}')
                print()
                print(f'{beta.iloc[row_idx]}')


        if value_diff_count > 0:
            differences.append(f'found {value_diff_count} value differences in {alpha_row_count} records, across columns: {",".join(cols_with_val_diffs)}')

    value_delta()
   
    identical = len(differences) == 0

    if not identical:
        print('DIFFERENCES:')
        [print(difference) for difference in differences]

    return identical

def diff(
        alpha_label: str, alpha_path: str, 
        beta_label: str, beta_path: str,
        in_place_remediations: Callable[[DataFrame], None],
    ):

    print('loading from file ...')

    alpha_df = load_dataframe_from_file(alpha_path, compression='zip')
    beta_df = load_dataframe_from_file(beta_path, compression='gzip')

    if in_place_remediations is not None:
        print(f'applying in-place remediations to {alpha_label} ...')
        in_place_remediations(alpha_df)

    # align

    sort_order = [ 
        'main_start_date', 
        'kk_date', 
        'trip_id', 
        'start_date', 
        'dossier_code', 
        'actual_trip_code', 
        'core_trip_code' 
    ] 
    
    for df in [alpha_df, beta_df]:
        df.sort_values(by=sort_order, inplace=True)

    # compare
        
    print('comparing ...')
        
    identical = compare(alpha_label, alpha_df, beta_label, beta_df)
    if identical:
        print('data frames are identical')
    else:
        print('data frames are NOT identical')

    return identical

args = sys.argv[1:]

alpha_label = args[0] 
alpha_path = args[1]

beta_label = args[2]
beta_path = args[3]

print(f'comparing {alpha_label} and {beta_label}')
print(f'{alpha_label} @ {alpha_path}')
print(f'{beta_label} @ {beta_path}')

def in_place_remediations_to_alpha(alpha_df: DataFrame):
    alpha_df.replace('', None, inplace=True)
    #pass

print(diff(alpha_label, alpha_path, beta_label, beta_path, in_place_remediations_to_alpha))