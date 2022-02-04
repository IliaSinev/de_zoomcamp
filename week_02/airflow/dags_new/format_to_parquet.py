import os
import logging
import glob
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

def format_to_parquet(src_file):

    """
    Takes local csv file and converts it to parquet file in small portions
    :param src_file: local csv file to be converted
    """

    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return

    dst_folder = src_file.replace('.csv', '_parquet')
    filelist = glob.glob(f'{dst_folder}/*.parquet')

    # pyarrow generates unique names for chunked parquets -> deleting existing files 

    if os.path.exists(dst_folder) and len(filelist) != 0:
        print(f'target folder {dst_folder} is not empty, deleting {len(filelist)} files')
        for f in filelist:
            os.remove(f)
    
    df_iter = pd.read_csv(src_file, encoding_errors='ignore', on_bad_lines = 'warn', iterator = True, chunksize = 1000000)

    while True:
        try: 
            df = next(df_iter)
            table_pq = pa.Table.from_pandas(df)
            pq.write_to_dataset(table_pq, root_path=dst_folder)
            print(f'saved another {len(df)} lines to parquet file')
        except (StopIteration):
            print('EOF reached')
            break
