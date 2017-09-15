import json
from toflerdb.utils.common import Common


def insert_task(user_id, public_taskid, internal_taskid, task_queue_id,
                task_status, task_info):
    query = """
        INSERT INTO toflerdb_bulk_task_queue(user_id, public_taskid,
        internal_taskid, task_queue_id, task_status, task_info)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    query_data = (user_id, public_taskid, internal_taskid, task_queue_id,
                  task_status, json.dumps(task_info))
    Common.execute_query(query, query_data)


def get_task_by_id(taskid):
    query = """
        SELECT user_id, public_taskid, internal_taskid, task_queue_id,
        task_status, task_info FROM toflerdb_bulk_task_queue WHERE
        public_taskid = %s OR internal_taskid = %s
    """
    query_data = (taskid, taskid)
    response = Common.execute_query(query, query_data)
    if len(response) == 1:
        retval = response[0]
        retval['task_info'] = json.loads(retval['task_info'])
        return retval
    return {}


def update_task_status(public_taskid, new_status):
    query = """
        UPDATE toflerdb_bulk_task_queue SET task_status = %s
        WHERE public_taskid = %s
    """
    query_data = (new_status, public_taskid)
    Common.execute_query(query, query_data)


def update_task_info(public_taskid, new_info):
    query = """
        UPDATE toflerdb_bulk_task_queue SET task_info = %s
        WHERE public_taskid = %s
    """
    query_data = (json.dumps(new_info), public_taskid)
    Common.execute_query(query, query_data)


def get_tasks_of_user(user_id, task_queue_id):
    query = """
        SELECT user_id, public_taskid, internal_taskid, task_queue_id,
        task_status, task_info FROM toflerdb_bulk_task_queue WHERE
        user_id = %s AND task_queue_id = %s
    """
    query_data = (user_id, task_queue_id)

    return Common.execute_query(query, query_data)
