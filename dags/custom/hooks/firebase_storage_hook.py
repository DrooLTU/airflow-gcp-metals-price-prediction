from airflow.hooks.base import BaseHook
import firebase_admin
from firebase_admin import storage

class FirebaseStorageHook(BaseHook):
    def __init__(self, firebase_conn_id='firebase_default'):
        self.firebase_conn_id = firebase_conn_id
        self._client = None

    def get_conn(self):
        if not self._client:
            conn = self.get_connection(self.firebase_conn_id)
            cred = firebase_admin.credentials.Certificate(conn.extra_dejson.get('key_path'))
            firebase_admin.initialize_app(cred)
            self._client = storage.bucket(f'{conn.extra_dejson.get("project")}.appspot.com')
        return self._client