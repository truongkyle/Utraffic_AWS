# config S3 env
import boto3
import config as CONFIG
import os
"""
S3 ENVIRONMENT
SUMMARY:
    1. Download from s3         - download_file_from_s3
    2. Upload to s3             - upload_file_to_s3
    3. Remove local file        - remove_local_file
    4. Get file name from S3Key - get_file_name_from_s3 
    5. Get folder from S3Key    - get_folder_name_from_s3
    6. Delete file in s3        - delete_files_in_s3
    7. Detect local file        - check_local_file
"""
# s3_bucket_folder = CONFIG.S3_BUCKET

class ConfigS3:
    def __init__(self):
        self.s3 = boto3.resource('s3',
                    region_name=CONFIG.S3_REGION,
                    aws_access_key_id=CONFIG.S3_ACCESS_KEY,
                    aws_secret_access_key=CONFIG.S3_SECRET_ACCESS_KEY
                    )
        self.bucket = self.s3.Bucket(CONFIG.S3_BUCKET)
        
    def download_file_from_s3(self, local_file_path, s3_key):
        try:
            self.remove_local_file(local_file_path)
            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))
            self.bucket.download_file(Key=s3_key,
                                    Filename=local_file_path)
            return local_file_path
        except KeyError:
            os.rmdir(os.path.dirname(local_file_path))  # remove the file
            return

    def upload_file_to_s3(self, local_file_path, s3_key):
        try:
            self.bucket.upload_file(Key=s3_key,
                                    Filename=local_file_path)
        except KeyError:
            return
    def remove_local_file(self, local_file_path):
        try:  
            if os.path.exists(os.path.dirname(local_file_path)):          
                os.remove(local_file_path)
                os.rmdir(os.path.dirname(local_file_path))  # remove the file
            os.remove(local_file_path)
        except Exception:            
            pass

    def get_file_name_from_s3(self, s3_key):
        filename = os.path.basename(s3_key)
        return filename

    def get_folder_name_from_s3(self, s3_key):
        foldername = os.path.dirname(s3_key)
        return foldername
    
    def get_folder_from_s3(self, folder_name):
        for obj in self.bucket.objects.filter(Prefix=folder_name):
            if not os.path.exists(os.path.dirname(obj.key)):
                os.makedirs(os.path.dirname(obj.key))
            try:
                self.bucket.download_file(obj.key, obj.key)
            except Exception:
                pass

    def delete_files_in_s3(self, s3_key):
        try:
            self.bucket.Object(s3_key).delete()
        except Exception:
            pass

    def check_local_file(self, local_s3_key):
        local_file = os.path.isfile(local_s3_key)
        return local_file

    def s3_unit(self):
        return self.bucket
"""
MySQL ENVIRONMENT
SUMMARY:
    1. Setup AWS-CONFIG.DB             - mycursors
    2. Get PipelineID           - get_pipeline_id
    3. Check status of progress - status_checking
    4. Check progress           - checking_progress 
    5. Failed progress status   - failed_checking_progress
    6. Complete status          - completed
"""
DEFINE_SELECT_ID = "SELECT * from tbl_pipeline WHERE id = %s"
