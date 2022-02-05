from google.cloud import storage
import glob

def upload(bucket, dataset_name, local_pq_folder):
    """
    Uploads parquet files from local folder to Google Cloud Storage bucket
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param dataset_name: template name for target path & file-name
    :param local_pq_folder: local source path
    :return:
    """

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    local_files = glob.glob(f'{local_pq_folder}*.parquet')
    print(f'there are {len(local_files)} files in {local_pq_folder}: {local_files}')
    blobs = list(bucket.list_blobs(prefix=f'raw/{dataset_name}_parquet/'))
    if len(blobs) != 0:
        print(f'target bucket already conatins {len(blobs)} blobs: deleting')
        for blob in blobs:
            blob.delete()
   
    for i, file in enumerate(local_files):
        enum = f'00{i+1}'[-3:]
        dst_blob = f'raw/{dataset_name}_parquet/{dataset_name}_{enum}.parquet'       
        blob = bucket.blob(dst_blob)
        print(f'uploading {file} to {dst_blob}')
        blob.upload_from_filename(file)