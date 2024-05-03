'''load two dataframes from file (pickle), and then systematically compare them'''

import argparse
from typing import List

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

    print(f'... loaded')

    return df

def are_equal_null_values(alpha: any, beta: any):
    return pandas.isnull(alpha) and pandas.isnull(beta)

def compare(
        ref_label: str, ref_df: DataFrame, 
        target_label: str, target_df: DataFrame,
        sort_order: List[str]
    ) -> bool:
    
    differences = []

    # row count

    ref_row_count = len(ref_df.index)
    target_row_count = len(target_df.index)

    if ref_row_count != target_row_count:
        differences.append(f'row counts do not match: {ref_label} = {ref_row_count}, {target_label} = {target_row_count}')
    else:
        print(f'row count at {ref_row_count} is consistent across both')

    # column count
        
    ref_cols = ref_df.columns.to_list()
    target_cols = target_df.columns.to_list()

    ref_col_count = len(ref_cols)
    target_col_count = len(target_cols)

    if ref_col_count != target_col_count:
        differences.append(f'column counts do not match: {ref_label} = {ref_col_count}, {target_label} = {target_col_count}')
    else:
        print(f'{ref_col_count} columns: {", ".join(ref_cols)}')

    # column name
        
    column_union = set(ref_cols + target_cols)
    column_intersection = [col for col in column_union if col in ref_cols and col in target_cols]
    column_disjoint = [col for col in column_union if col not in column_intersection]

    if len(column_disjoint) > 0:
        differences.append(f'disjoint columns: {"".join(column_disjoint)}')

    for i in range(ref_col_count):
        if ref_cols[i] != target_cols[i]:
            differences.append(f'column order does not match: {i} {ref_label} {ref_cols[i]}, {target_label} {target_cols[i]}')

    # column types

    for i in range(ref_col_count):
        alpha_dtype = ref_df.dtypes.iloc[i]
        beta_dtype = target_df.dtypes.iloc[i]
        alpha_col_name = ref_cols[i]
        beta_col_name = target_cols[i]
        if alpha_dtype != beta_dtype:
            differences.append(f'column types do not match: {ref_label} {alpha_col_name} {alpha_dtype}, {target_label} {beta_col_name} {beta_dtype}')

    # --------------------------------------
                
    def value_delta():
        cell_difference_count = 0
        row_diff_count = 0

        cols_with_val_diffs = []

        for row_idx in range(ref_row_count):

            row_has_a_difference = False

            # only log differences for the first differing row
            #
            log_value_differences = row_diff_count == 0

            for col_idx in range(ref_col_count):

                col = ref_cols[col_idx]
                alpha_val = ref_df[col].iloc[row_idx]
                beta_val = target_df[col].iloc[row_idx]

                # detect superficial difference
                #
                if alpha_val != beta_val:

                    row_has_a_difference = True
                    
                    # exclude null casess
                    #
                    if not are_equal_null_values(alpha_val, beta_val): 
                            
                        cell_difference_count = cell_difference_count + 1
                        
                        if col not in cols_with_val_diffs:
                            cols_with_val_diffs.append(col)
                        
                        if log_value_differences:
                            
                            text = f'value difference in row {row_idx} column {col}: {ref_label} ({type(alpha_val)}) : {alpha_val} compare {target_label} ({type(beta_val)}) : {beta_val}'
                            differences.append(text)

            if row_has_a_difference:
                
                row_diff_count = row_diff_count + 1
                
                if log_value_differences:
                    print('=-'*40)
                    print(f'{ref_label}')
                    print()
                    print(f'{ref_df.iloc[row_idx]}')

                    print('=-'*40)
                    print(f'{target_label}')
                    print()
                    print(f'{target_df.iloc[row_idx]}')

                    print('abandoning checking for further value differences after first row')
                    break

        if cell_difference_count > 0:
            differences.append(f'found {cell_difference_count} value differences in {ref_row_count} records, across columns: {",".join(cols_with_val_diffs)}, in {row_diff_count} rows')

    value_delta()
   
    identical = len(differences) == 0

    if not identical:
        print()
        print('DIFFERENCES:')
        [print(difference) for difference in differences]

    return identical

def diff(
        ref_label: str, 
        ref_path: str, 
        ref_compression: str,
        
        target_label: str, 
        target_path: str, 
        target_compression: str,
        
        sort_order: List[str],
        in_place_remediations_to_target: Callable[[DataFrame], None] = None
    ):

    ref_df = load_dataframe_from_file(ref_path, compression=ref_compression) # zip
    target_df = load_dataframe_from_file(target_path, compression=target_compression) # # gzip

    # remediations

    if in_place_remediations_to_target:
        in_place_remediations_to_target(target_df)

    # sort to align rows

    ascending_order = [True for _ in sort_order]

    print(f'sorting dataframe rows in order of {", ".join(sort_order)}')
    ref_df.sort_values(by=sort_order, ascending=ascending_order, inplace=True)
    target_df.sort_values(by=sort_order, ascending=ascending_order, inplace=True)
    
    # compare
        
    print('comparing ...')
        
    identical = compare(ref_label, ref_df, target_label, target_df)
    if identical:
        print('data frames are identical')
    else:
        print('data frames are NOT identical')

    # id = 876439

    # print(ref_label)
    # ref_matches = ref_df[ref_df.core_trip_id.isin([id])] 
    # print('ref matches: {}'.format(len(ref_matches.index)))

    # print(ref_df.head(10)['core_trip_id'])

    # print(target_label)
    # target_matches = target_df[target_df.core_trip_id.isin([id])]
    # print('target matches: {}'.format(len(target_matches.index)))

    # target_df.head(10)
    # print(target_df.head(10)['core_trip_id'])

    return identical

# ---------
REF_COMPRESSION = 'ref_compression'
REF_LABEL = 'ref_label'
REF_PATH = 'ref_path'
SORT_ORDER = 'sort_order'
TARGET_COMPRESSION = 'target_compression'
TARGET_LABEL = 'target_label'
TARGET_PATH = 'target_path'

def parse_args():
    parser = argparse.ArgumentParser(description='produce a readable summary of the delta of two pandas dataframes')

    parser.add_argument(f'--{REF_PATH}', help='file path to reference df', required=True)
    parser.add_argument(f'--{REF_COMPRESSION}', help='compression used when pickling reference DF', required=True)
    parser.add_argument(f'--{REF_LABEL}', help='label of reference df', default='reference', required=False)

    parser.add_argument(f'--{TARGET_PATH}', help='file path to target df to compare against reference', required=True)
    parser.add_argument(f'--{TARGET_COMPRESSION}', help='compression used when pickling target DF', required=True)
    parser.add_argument(f'--{TARGET_LABEL}', help='label of target df', default='target', required=False)

    parser.add_argument(f'--{SORT_ORDER}', help='csv list of column names in which order to sort dfs before comparison', required=True)

    return vars(parser.parse_args())

args = parse_args()
identical = diff(
    args[REF_LABEL], args[REF_PATH], args[REF_COMPRESSION],
    args[TARGET_LABEL], args[TARGET_PATH], args[TARGET_COMPRESSION],
    sort_order = args[SORT_ORDER].split(','),
    in_place_remediations_to_target=lambda df: df.replace('', None, inplace=True)
)