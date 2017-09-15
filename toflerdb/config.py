from toflerdb.utils.common import Common


Q_BULK_UPLOAD_FACTS = 'BULK_UPLOAD_FACTS'
SNAPSHOT_INDEX = Common.get_config('elastic')['index']
SNAPSHOT_DOC_TYPE = Common.get_config('elastic')['doc_type']


class FACT_STATUS:
    ACTIVE = 'A'
    PENDING = 'P'
    DELETED = 'D'


class UPLOAD_TASK_STATUS:
    UPLOADED = 'U'
    PROGRESS = 'P'
    COMPLETED = 'C'
    FAILED = 'F'


UPLOAD_TASK_STATUS_LABEL = {
    UPLOAD_TASK_STATUS.UPLOADED: 'PENDING',
    UPLOAD_TASK_STATUS.PROGRESS: 'PROGRESS',
    UPLOAD_TASK_STATUS.COMPLETED: 'COMPLETED',
    UPLOAD_TASK_STATUS.FAILED: 'FAILED'
}
