from airflow.models import BaseOperator
from custom.hooks.firebase_storage_hook import FirebaseStorageHook

class FirebaseStorageWriteOperator(BaseOperator):
    """
    Custom operator to do operations read/write on Firebase storage bucket.
    """
    template_fields = ("_data", "_filename", "_dir")
    
    def __init__(self, data:str =None, filename:str =None, dir:str =None, content_type:str =None, rewrite:bool =False, *args, **kwargs):
        """
        Initialize the operator.

        Args:

        """
        super().__init__(*args, **kwargs)
        self._data = data
        self._filename = filename
        self._dir = dir
        self._content_type = content_type
        self._rewrite = rewrite

    def execute(self, context):
        """
        Execute the operator.
        """
        # Retrieve Firebase Authentication token from the Airflow connection

        # Create a Firebase Cloud Functions hook
        fsh = FirebaseStorageHook()

        response = fsh.write_data(self._data, self._filename, self._dir, self._content_type, self._rewrite)

        self.log.info(f'Firebase store write: {response.text}')