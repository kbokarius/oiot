import os, sys, traceback, binascii
import json, random, string, datetime, uuid
from datetime import datetime

# collection name to use for the locks collection
_locks_collection = 'oiot-locks'

# collection name to use for the jobs collection
_jobs_collection = 'oiot-jobs'

# collection name to use for the curators collection
_curators_collection = 'oiot-curators'

# collection key name to use for the active curator
_active_curator_key = 'active'

# minimum heartbeat interval for curators
_curator_heartbeat_interval_in_ms = 500

# elapsed time before a curator times out and another should take its place
_curator_heartbeat_timeout_in_ms = 7500

# time an inactive curator should sleep between status checks
_curator_inactivity_delay_in_ms = 3000

# elapsed time before a job is timed out and automatically rolled back
_max_job_time_in_ms = 5000

# additional elapsed time used by active curators before rolling back jobs
_additional_timeout_wait_in_ms = 1000

# value used by journal items to indicate a delete operation was performed
_deleted_object_value = {"deleted": "{A0981677-7933-4A5C-A141-9B40E60BD411}"}

def _generate_key():
    """
    Generate a random 16 character alphanumeric string.
    :return: a random 16 character alphanumeric string.
    """
    return str(binascii.b2a_hex(os.urandom(8)))

def _get_lock_collection_key(collection_to_lock, key_to_lock):
    """
    Get the key used for the locks collection based on the specified
    collection name key.
    :param collection_to_lock: the collection name to lock
    :param key_to_lock: the key to lock
    :return: the formatted locks collection key
    """
    return collection_to_lock + "-" + str(key_to_lock)

def _get_httperror_status_code(exception):
    """
    Get the HTTPError status code from the specified exception.
    :param exception: the specified exception
    :return: the HTTPError status code
    """
    if exception.__class__.__name__ is 'HTTPError':
        return exception.response.status_code
    else:
        return None

def _format_exception(e):
    """
    Format the specified exception into a readable string.
    :param exception: the specified exception
    :return: the formatted exception
    """
    return traceback.format_exc()

def _create_and_add_lock(client, collection, key, job_id, timestamp):
    """
    Create and add a lock to the locks collection. This will instantiate a
    lock object, add its details to the locks collection in o.io, and return
    the lock instance.
    :param client: the client to use
    :param collection: the collection name
    :param key: the key
    :param job_id: the job ID
    :param timestamp: the timestamp
    :return: the created lock
    """
    lock = _Lock(job_id, timestamp, datetime.utcnow(), 
            collection, key, None)
    lock_response = client.put(_locks_collection, 
            _get_lock_collection_key(collection, key), 
            json.loads(json.dumps(vars(lock), cls=_Encoder)), 
            False, False)
    if lock_response.status_code == 412:
        raise CollectionKeyIsLocked
    lock_response.raise_for_status()
    lock.lock_ref = lock_response.ref
    return lock

def _roll_back_journal_item(client, journal_item, raise_if_timed_out):
    """
    Roll back the specified journal item.
    :param client: the client to use
    :param journal_item: the journal item to roll back
    :param raise_if_timed_out: the method to call if the roll back times out
    """
    # Don't attempt to roll-back if the original value and the
    # new value are the same.
    if journal_item.original_value == journal_item.new_value:
        return
    raise_if_timed_out()
    was_objected_deleted = journal_item.new_value == _deleted_object_value
    get_response = client.get(journal_item.collection,
            journal_item.key, None, False)
    try: 
        get_response.raise_for_status()
    except Exception as e:
        if _get_httperror_status_code(e) == 404:
            if was_objected_deleted is False:
                return
        else:
            raise e
    # Don't attempt to roll-back if the new value does not match
    # unless the record was deleted by the job.
    if (was_objected_deleted is False and
            get_response.json != journal_item.new_value):
        return
    # Was there an original value?
    if journal_item.original_value:
        # Put back the original value only if the new
        # value matches or if the record was deleted by 
        # the job.
        if ((was_objected_deleted and
                get_response.status_code == 404) or 
                (was_objected_deleted is False and
                get_response.json == journal_item.new_value)):
            original_ref = False
            if was_objected_deleted is False:
                original_ref = get_response.ref
            raise_if_timed_out()
            try: 
                put_response = client.put(
                        journal_item.collection,
                        journal_item.key, 
                        journal_item.original_value, 
                        original_ref, False)
                put_response.raise_for_status()
            except Exception as e:
                # Ignore 412 error if the ref did not match.
                if (_get_httperror_status_code(e) == 412):
                    return
                else:
                    raise e
    # No original value indicates that a new record was 
    # added and should be deleted.
    else:
        raise_if_timed_out()
        try:
            delete_response = client.delete(
                    journal_item.collection,
                    journal_item.key, 
                    get_response.ref, False)
            delete_response.raise_for_status()
        except Exception as e:
            # Ignore 412 error if the ref did not match.
            if (_get_httperror_status_code(e) == 412):
                return
            else:
                raise e

class _ActiveCuratorDetails(object):
    """
    Represents the details about the currently active curator.
    """
    def __init__(self, curator_id = None, timestamp = None):
        """
        Create an ActiveCuratorDetails instance.
        :param curator_id: the curator ID
        :param timestamp: the heartbeat timestamp
        """
        self.curator_id = curator_id
        self.timestamp = timestamp

class _Lock(object):
    """
    Represents a read-write lock and its information.
    """
    def __init__(self, job_id = None, job_timestamp = None, timestamp = None,
                collection = None, key = None, lock_ref = None):
        """
        Create a Lock instance.
        :param job_id: the job ID
        :param job_timestamp: the job timestamp
        :param timestamp: the lock timestamp
        :param collection: the collection
        :param key: the key
        :param lock_ref: the o.io ref value for the lock object
        """
        self.job_id = job_id
        self.job_timestamp = job_timestamp
        self.timestamp = timestamp
        self.collection = collection
        self.key = key
        self.lock_ref = lock_ref

class _JournalItem(object):
    """
    Represents a journal item and its information.
    """
    def __init__(self, timestamp = None, collection = None, key = None,
                original_value = None, new_value = None):
        """
        Create a JournalItem instance.
        :param timestamp: the timestamp
        :param collection: the collection
        :param key: the key
        :param original_value: the original value
        :param new_value: the new value
        """
        self.timestamp = timestamp
        self.collection = collection
        self.key = key
        self.original_value = original_value
        self.new_value = new_value

class _Encoder(json.JSONEncoder):
    """
    Determines how to properly encode objects into JSON.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, _JournalItem):
            return vars(obj)
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class CollectionKeyIsLocked(Exception):
    """
    Raised when a locked key is accessed.
    """
    pass

class FailedToComplete(Exception):
    """
    Raised when a job fails to complete.
    """
    def __init__(self, exception_failing_completion=None,
                stacktrace_failing_completion=None):
        """
        Create a FailedToComplete instance.
        :param exception_failing_completion: the exception that caused the
        job completion to fail
        :param stacktrace_failing_completion: the stacktrace that caused the
        job completion to fail
        """
        super(FailedToComplete, self).__init__()
        self.exception_failing_completion = exception_failing_completion
        self.stacktrace_failing_completion = stacktrace_failing_completion

class FailedToRollBack(Exception):
    """
    Raised when a job fails to roll back.
    """
    def __init__(self, exception_causing_rollback=None,
                stacktrace_causing_rollback=None,
                exception_failing_rollback=None,
                stacktrace_failing_rollback=None):
        """
        Create a FailedToRollBack instance.
        :param exception_causing_rollback: the exception that caused the
        job to roll back
        :param stacktrace_causing_rollback: the stacktrace that caused the
        job to roll back
        :param exception_failing_rollback: the exception that caused the
        job roll back to fail
        :param stacktrace_failing_rollback: the stacktrace that caused the
        job roll back to fail
        """
        super(FailedToRollBack, self).__init__()
        self.exception_causing_rollback = exception_causing_rollback
        self.stacktrace_causing_rollback = stacktrace_causing_rollback
        self.exception_failing_rollback = exception_failing_rollback
        self.stacktrace_failing_rollback = stacktrace_failing_rollback

class RollbackCausedByException(Exception):
    """
    Raised when a roll back is caused by an exception.
    """
    def __init__(self, exception_causing_rollback=None,
                stacktrace_causing_rollback=None):
        """
        Create a RollbackCausedByException instance.
        :param exception_causing_rollback: the exception that caused the
        job to roll back
        :param stacktrace_causing_rollback: the stacktrace that caused the
        job to roll back
        """
        super(RollbackCausedByException, self).__init__()
        self.exception_causing_rollback = exception_causing_rollback
        self.stacktrace_causing_rollback = stacktrace_causing_rollback

class JobIsFailed(Exception):
    """
    Raised when a failed job is used.
    """
    pass

class JobIsCompleted(Exception):
    """
    Raised when a completed job is used.
    """
    pass

class JobIsRolledBack(Exception):
    """
    Raised when a rolled back job is used.
    """
    pass

class JobIsTimedOut(Exception):
    """
    Raised when a timed out job is used.
    """
    pass

class CuratorNoLongerActive(Exception):
    """
    Raised when an active curator is no longer active.
    """
    pass

from .client import OiotClient
from .job import Job
from .curator import Curator
