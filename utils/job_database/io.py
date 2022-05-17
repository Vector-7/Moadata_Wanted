from libs.resource_access import RawFileRead, RawFileWrite

JOB_DATABASE_ROOT = 'storage/jobs.json'


class JobDatabaseRead(RawFileRead):

    def __init__(self):
        super().__init__(JOB_DATABASE_ROOT)


class JobDatabaseWrite(RawFileWrite):

    def __init__(self):
        super().__init__(JOB_DATABASE_ROOT)
