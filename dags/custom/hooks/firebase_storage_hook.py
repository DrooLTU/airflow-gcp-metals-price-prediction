from airflow.hooks.base import BaseHook
import firebase_admin
from firebase_admin import storage

class FirebaseStorageHook(BaseHook):

    def __init__(self, firebase_conn_id='firebase_default'):
        self.firebase_conn_id = firebase_conn_id
        self.bucket = None
        self._fb_app = None

    def get_conn(self):
        if not self.bucket:
            conn = self.get_connection(self.firebase_conn_id)
            cred = firebase_admin.credentials.Certificate(conn.extra_dejson.get('key_path'))
            self._fb_app = firebase_admin.initialize_app(cred)
            self.bucket = storage.bucket(f'{conn.extra_dejson.get("project")}.appspot.com')
        return self.bucket
    
    
    def write_data(self, data:str, filename:str, dir:str, content_type: str = "text/plain", rewrite:bool = False):
        """
        Writes data to cached client
        """
        
        storage_filename = f'{dir}/{filename}'

        blob = self.bucket.blob(storage_filename)
        exists = blob.exists()

        if exists and not rewrite:
            return f"The file {storage_filename} exists in Firebase Storage."
        else:
            blob.upload_from_string(data, content_type=content_type)
            return f'File saved! {blob.name}'


    def read_data(self, filename:str, dir:str) -> str:
        """
        Reads GCS file data and returns it as serialized string.
        """

        storage_filename = f'{dir}/{filename}'

        blob = self.bucket.blob(storage_filename)
        exists = blob.exists()

        if exists:
            return blob.download_as_text()
            
        else:
            print(f"The file {filename} DOES NOT exist in Firebase Storage.")