import os, sys, traceback, binascii, json, random, string, \
        datetime, uuid
from datetime import datetime
from .settings import _locks_collection, _jobs_collection, \
        _max_job_time_in_ms, _deleted_object_value
from .exceptions import JobIsRolledBack, JobIsFailed, FailedToComplete, \
        FailedToRollBack, RollbackCausedByException, JobIsTimedOut, \
        CollectionKeyIsLocked, JobIsCompleted, _get_httperror_status_code    

# TODO: Add fields to the roll back exceptions to indicate what the original
# exception and stack trace was.

class Job:
    """
    A class used for executing o.io operations as a single atomic
    transaction by utilizing locking and journaling mechanisms.
    """
    def __init__(self, client):
        """
        Create a Job instance.
        :param client: the client to use
        """
        self._job_id = Job._generate_key()
        self._timestamp = datetime.utcnow()
        self._client = client
        self._locks = []
        self._journal = []
        self.is_completed = False
        self.is_rolled_back = False
        # A job should fail only in the event of an exception during
        # completion or roll-back.
        self.is_failed = False

    @staticmethod
    def _generate_key():
        """
        Generate a random 16 character alphanumeric string.
        :return: a random 16 character alphanumeric string.
        """
        return str(binascii.b2a_hex(os.urandom(8)))

    @staticmethod
    def _get_lock_collection_key(collection_to_lock, key_to_lock):
        """
        Get the key used for the locks collection based on the specified
        collection name key.
        :param collection_to_lock: the collection name to lock
        :param key_to_lock: the key to lock
        :return: the formatted locks collection key
        """
        return collection_to_lock + "-" + str(key_to_lock)

    @staticmethod
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
                Job._get_lock_collection_key(collection, key),
                json.loads(json.dumps(vars(lock), cls=_Encoder)),
                False, False)
        if lock_response.status_code == 412:
            raise CollectionKeyIsLocked
        lock_response.raise_for_status()
        lock.lock_ref = lock_response.ref
        return lock

    @staticmethod
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

    def _verify_job_is_active(self):
        """
        Verify that this job is active and raise an exception if it is not.
        """
        if self.is_failed:
            raise JobIsFailed
        elif self.is_completed:
            raise JobIsCompleted
        elif self.is_rolled_back:
            raise JobIsRolledBack
        self._raise_if_job_is_timed_out()

    def _raise_if_job_is_timed_out(self):
        """
        Verify that this job is not timed out and raise an exception
        if it is.
        """
        elapsed_milliseconds = (datetime.utcnow() -
                self._timestamp).total_seconds() * 1000.0
        if elapsed_milliseconds > _max_job_time_in_ms:
            raise JobIsTimedOut('Ran for ' + str(elapsed_milliseconds) + 'ms')

    def _remove_locks(self):
        """
        Remove all locks associated with this job from o.io.
        """
        for lock in self._locks:
            self._raise_if_job_is_timed_out()
            response = self._client.delete(_locks_collection,
                    Job._get_lock_collection_key(lock.collection, lock.key),
                    lock.lock_ref, False)
            response.raise_for_status()
        self._locks = []

    def _remove_job(self):
        """
        Remove this job from o.io.
        """
        self._raise_if_job_is_timed_out()
        response = self._client.delete(_jobs_collection, self._job_id,
                None, False)
        response.raise_for_status()
        self._journal = []

    def _get_lock(self, collection, key):
        """
        Create a lock for the specified collection and key and add
        it to o.io.
        :param collection: the specified collection to lock
        :param key: the specified key to lock
        :return: the created lock
        """
        for lock in self._locks:
            if lock.collection == collection and lock.key == key:
                return lock
        self._raise_if_job_is_timed_out()
        lock = Job._create_and_add_lock(self._client, collection, key,
                self._job_id, self._timestamp)
        self._locks.append(lock)
        return lock

    def _add_journal_item(self, collection, key, new_value, original_value):
        """
        Add a journal item to this job.
        :param collection: the collection
        :param key: the key
        :param new_value: the new value
        :param original_value: the original value
        :return: the created journal item
        """
        self._raise_if_job_is_timed_out()
        journal_item = _JournalItem(datetime.utcnow(), collection, key,
                original_value, new_value)
        self._journal.append(journal_item)
        job_response = self._client.put(_jobs_collection, self._job_id,
                json.loads(json.dumps({'timestamp': self._timestamp,
                'items': self._journal}, cls=_Encoder)), None, False)
        job_response.raise_for_status()
        return journal_item

    def get(self, collection, key, ref = None):
        """
        Execute a get operation via this job by locking the collection key
        prior to executing the operation.
        :param collection: the collection
        :param key: the key
        :param ref: the ref
        :return: the operation's response
        """
        self._verify_job_is_active()
        try:
            lock = self._get_lock(collection, key)
            self._raise_if_job_is_timed_out()
            response = self._client.get(collection, key, ref, False)
            response.raise_for_status()
            self._raise_if_job_is_timed_out()
            return response
        except Exception as e:
            self.roll_back((e, traceback.format_exc()))

    def post(self, collection, value):
        """
        Execute a post operation via this job by locking the collection key
        prior to executing the operation.
        :param collection: the collection
        :param key: the key
        :param ref: the ref
        :return: the operation's response
        """
        key = Job._generate_key()
        return self.put(collection, key, value)

    def put(self, collection, key, value, ref = None):
        """
        Execute a put operation via this job by locking the collection key
        prior to executing the operation.
        :param collection: the collection
        :param key: the key
        :return: the operation's response
        """
        self._verify_job_is_active()
        try:
            lock = self._get_lock(collection, key)
            self._raise_if_job_is_timed_out()
            # If ref was passed, ensure that the value has not changed.
            # If ref was not passed, retrieve the current ref and store it.
            response = self._client.get(collection, key, ref, False)
            # Indicates a new record will be created.
            if response.status_code == 404:
                original_value = None
            # Indicates an existing record will be updated so store the
            # original ref and value.
            elif response.status_code == 200:
                original_value = response.json
            else:
                response.raise_for_status()
            journal_item = self._add_journal_item(collection, key,
                    value, original_value)
            self._raise_if_job_is_timed_out()
            response = self._client.put(collection, key, value, ref, False)
            response.raise_for_status()
            self._raise_if_job_is_timed_out()
            return response
        except Exception as e:
            self.roll_back((e, traceback.format_exc()))

    def delete(self, collection, key, ref = None):
        """
        Execute a delete operation via this job by locking the collection key
        prior to executing the operation.
        :param collection: the collection
        :param key: the key
        :return: the operation's response
        """
        self._verify_job_is_active()
        try:
            lock = self._get_lock(collection, key)
            self._raise_if_job_is_timed_out()
            # The record must be present in order to delete it.
            response = self._client.get(collection, key, ref, False)
            response.raise_for_status()
            original_value = response.json
            journal_item = self._add_journal_item(collection, key,
                    _deleted_object_value, original_value)
            self._raise_if_job_is_timed_out()
            response = self._client.delete(collection, key, ref, False)
            response.raise_for_status()
            self._raise_if_job_is_timed_out()
            return response
        except Exception as e:
            self.roll_back((e, traceback.format_exc()))

    def roll_back(self, exception_causing_rollback = None):
        """
        Rolls back this job by rolling back each journal item and removing
        the locks associated with the job and the job itself.
        :param exception_causing_rollback: the exception that caused
        the roll back
        """
        self._verify_job_is_active()
        try:
            for journal_item in self._journal:
                Job._roll_back_journal_item(self._client, journal_item,
                        self._raise_if_job_is_timed_out)
            self._remove_job()
            self._remove_locks()
            self.is_rolled_back = True
            if exception_causing_rollback:
                raise RollbackCausedByException(exception_causing_rollback[0],
                        exception_causing_rollback[1])
        except RollbackCausedByException as e:
            raise e
        except Exception as e:
            self.is_failed = True
            if exception_causing_rollback:
                raise FailedToRollBack(e, traceback.format_exc(),
                        exception_causing_rollback[0],
                        exception_causing_rollback[1])
            else:
                raise FailedToRollBack(e, traceback.format_exc())

    def complete(self):
        """
        Completes this job by removing the locks associated with the job
        and the job itself.
        """
        self._verify_job_is_active()
        try:
            self._remove_job()
            self._remove_locks()
            self.is_completed = True
        except Exception as e:
            self.is_failed = True
            raise FailedToComplete(e, traceback.format_exc())


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
