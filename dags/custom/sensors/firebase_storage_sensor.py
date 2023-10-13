from airflow.sensors.base import BaseSensorOperator
from custom.hooks.firebase_storage_hook import FirebaseStorageHook
 
class FirebaseStorageSensor(BaseSensorOperator):
    """
    Sensor that waits for JSONL to be generated
    in specified folder of Firebase storage.

    folder : str
        the folder to check whether JSONL file exists in.
    """
    template_fields = ("_folder")

    def __init__(self, folder:str, **kwargs):
        super().__init__(**kwargs)
        self._folder = folder


    def poke(self, context):
        """
        We need a custom hook to connect to Firebase Storage bucket.
        Scans through the given foder and returns true if at least one JSONL file exists.
        """

        hook = FirebaseStorageHook()

        bucket = hook.get_bucket()
        self.log.info(f'Sensing in folder : {self._folder}')
        blobs = bucket.list_blobs(prefix=self._folder)

        if blobs:
            for blob in blobs:
                if(blob.content_type == 'application/jsonl'):
                    return True
                    
        return False
