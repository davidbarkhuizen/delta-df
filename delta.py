from pandas import read_pickle, DataFrame

# load two dataframes from file (pickle)
# systematically compare them

PICKLE_COMPRESSION_METHOD='zip'

import logging

def load_dataframe_from_file(file_path: str) -> DataFrame:

    print(f'loading dataframe from pkl file at {file_path} ...')

    df = read_pickle(
        filepath_or_buffer=file_path,
        compression=PICKLE_COMPRESSION_METHOD
    )

    print(f'... loaded dataframe from pkl file at {file_path}')


# pandas.DataFrame.compare
# DataFrame.compare(other, align_axis=1, keep_shape=False, keep_equal=False, result_names=('self', 'other'))[source]

def compare(alpha: DataFrame, beta: DataFrame) -> bool:
    differences = []
    
    return len(differences) == 0

local_file_path = '/Users/david.barkhuizen/development/lumos/mount/celery-worker-heavy/etl_input_set.pkl'
alpha = load_dataframe_from_file(local_file_path)
print(compare(alpha, alpha))
