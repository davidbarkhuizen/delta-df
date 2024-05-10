'''load two dataframes from file (pickle), and then systematically compare them'''

import argparse
from typing import List
import time

from pandas import NaT, read_pickle, DataFrame
#import logging
import pandas
import sys
from collections.abc import Callable

def load_dataframe_from_file(file_path: str, compression: str) -> DataFrame:

    print(f'loading dataframe from pkl file at {file_path} ...')

    started_at = time.process_time()

    df = read_pickle(
        filepath_or_buffer=file_path,
        compression=compression
    )

    elapsed_s = time.process_time() - started_at

    print(f'... loaded (in {elapsed_s} s)')

    return df

def are_equal_null_values(alpha: any, beta: any):
    return pandas.isnull(alpha) and pandas.isnull(beta)

def compare(
        ref_label: str, ref_df: DataFrame, 
        target_label: str, target_df: DataFrame
    ) -> bool:

    print('comparing...')

    started_at = time.process_time()
    
    differences = []

    # row count

    ref_row_count = len(ref_df.index)
    target_row_count = len(target_df.index)

    if ref_row_count != target_row_count:
        differences.append(f'row counts do not match: {ref_label} = {ref_row_count}, {target_label} = {target_row_count}')

    # column count
        
    ref_cols = ref_df.columns.to_list()
    target_cols = target_df.columns.to_list()

    ref_col_count = len(ref_cols)
    target_col_count = len(target_cols)

    if ref_col_count != target_col_count:
        differences.append(f'column counts do not match: {ref_label} = {ref_col_count}, {target_label} = {target_col_count}')

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
        ref_dtype = ref_df.dtypes.iloc[i]
        target_dtype = target_df.dtypes.iloc[i]
        col_name = ref_cols[i]
        if ref_dtype != target_dtype:
            differences.append(f'types do not match for col {col_name}: {ref_label} = {ref_dtype}, {target_label} = {target_dtype}')

    # --------------------------------------
                
    def value_delta():
        cell_difference_count = 0
        row_diff_count = 0

        cols_with_val_diffs = []
        row_idxs_to_detail = []

        l1 = 30
        l2 = 75

        row_diff_label_printed = False

        for row_idx in range(ref_row_count):

            row_has_a_difference = False

            for col_idx in range(ref_col_count):

                col = ref_cols[col_idx]
                ref_val = ref_df[col].iloc[row_idx]
                target_val = target_df[col].iloc[row_idx]

                # detect superficial difference
                #
                if ref_val != target_val:

                    if not are_equal_null_values(ref_val, target_val): 

                        row_has_a_difference = True

                        if not row_diff_label_printed:
                            differences.append(f'{"".rjust(l1)}{ref_label.ljust(l2)}{target_label.ljust(l2)}')
                            row_diff_label_printed = True
                            
                        cell_difference_count = cell_difference_count + 1
                        
                        if col not in cols_with_val_diffs:

                            if row_idx not in row_idxs_to_detail:
                                row_idxs_to_detail.append(row_idx)

                            cols_with_val_diffs.append(col)
                            
                            # text = f'value difference in row {row_idx} column {col}: {ref_label} ({type(alpha_val)}) : {alpha_val} compare {target_label} ({type(beta_val)}) : {beta_val}'
                            # differences.append(text)

                            label_str = f'{col} (row {row_idx})'
                            ref_str = f'{ref_val} ({type(ref_val)})'
                            target_str = f'{target_val} ({type(target_val)})'

                            differences.append(f'{label_str.ljust(l1)}{ref_str.ljust(l2)}{target_str.ljust(l2)}')

            if row_has_a_difference:
                row_diff_count = row_diff_count + 1
        
        col_diff_count = len(cols_with_val_diffs)

        if cell_difference_count > 0:
            differences.append(f'in total, found differences in {cell_difference_count} cells, {row_diff_count} rows, {col_diff_count} columns ({", ".join(cols_with_val_diffs)})')

        for row_idx in row_idxs_to_detail:

            differences.append('-'*80)
            differences.append(f'row: {row_idx}')
            differences.append(f'{"".rjust(l1)}{ref_label.ljust(l2)}{target_label.ljust(l2)}')
            differences.append('')

            for col_idx in range(ref_col_count):

                col = ref_cols[col_idx]

                ref_val = ref_df[col].iloc[row_idx]
                target_val = target_df[col].iloc[row_idx]

                label_str = f'{col} (row {row_idx})'
                ref_str = f'{ref_val} ({type(ref_val)})'
                target_str = f'{target_val} ({type(target_val)})'

                differences.append(f'{label_str.ljust(l1)}{ref_str.ljust(l2)}{target_str.ljust(l2)}')

            differences.append('')

    value_delta()
   
    identical = len(differences) == 0

    elapsed_s = time.process_time() - started_at
    print(f'... compared (in {elapsed_s} s)')

    if not identical:
        print()
        print('summary report'.upper())
        print()
        [print(difference) for difference in differences]
        print()

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
    # in_place_remediations_to_target=lambda df: df.replace('', None, inplace=True)
)