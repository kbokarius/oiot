from porc import Client
from .settings import _curators_collection, _locks_collection, \
        _active_curator_key, _curator_inactivity_delay_in_ms, \
        _curator_heartbeat_timeout_in_ms, _jobs_collection, \
        _curator_heartbeat_interval_in_ms, _max_job_time_in_ms, \
        _additional_timeout_wait_in_ms
from .job import _JournalItem, _Lock, _get_lock_collection_key, \
        _Encoder, _roll_back_journal_item
from .exceptions import _format_exception, _CuratorNoLongerActive, \
        _get_httperror_status_code

# TODO: Log unexpected exceptions locally and to 'oiot-errors'
# TODO: What to do if a job or journal is corrupt and can't be rolled back?

from datetime import datetime
import dateutil.parser
import uuid, time, json

class Curator(Client):
    """
    The class used for curating broken jobs and locks.
    """
    def __init__(self, client):
        """
        Create a Curator instance.
        :param client: the client to use
        """
        self._client = client
        self._id = uuid.uuid4()
        self._is_active = False
        self._last_heartbeat_time = None
        self._last_heartbeat_ref = None
        self._should_continue_to_run = True
        self._removed_job_ids = []

    def _append_to_removed_job_ids(self, job_id):
        """
        Append the specified job ID to the list of removed job IDs.
        :param job_id: the specified job ID
        """
        if len(self._removed_job_ids) > 1000:
            self._removed_job_ids = self._removed_job_ids[:750]
        self._removed_job_ids.append(job_id)

    def _make_inactive_and_sleep(self):
        """
        Make this curator instance inactive and sleep for some time.
        """
        self._is_active = False
        time.sleep(_curator_inactivity_delay_in_ms / 1000.0)

    def _try_send_heartbeat(self, add_new_record=False):
        """
        Try to send a heartbeat by updating the active curator object in the
        curators o.io collection.
        :param add_new_record: whether to add a new object to the curators
        o.io collection
        :return: whether the heartbeat was successfully sent
        """
        # If too little time has passed since the last heartbeat
        # then don't try to send another heartbeat.
        if (self._is_active and (datetime.utcnow() - self._last_heartbeat_time).
                total_seconds() * 1000.0 < _curator_heartbeat_interval_in_ms):
            return True
        last_ref_value = self._last_heartbeat_ref
        if add_new_record:
            last_ref_value = False
        active_curator_details = _ActiveCuratorDetails(self._id,
                datetime.utcnow())
        response = self._client.put(_curators_collection,
                _active_curator_key,
                json.loads(json.dumps(vars(active_curator_details),
                cls=_Encoder)), last_ref_value, False)
        try:
            response.raise_for_status()
        except Exception as e:
            # A 412 error indicates that another curator has become active.
            if (_get_httperror_status_code(e) == 412):
                if self._is_active:
                    raise _CuratorNoLongerActive
                return False
            else:
                raise e
        self._last_heartbeat_time = active_curator_details.timestamp
        self._last_heartbeat_ref = response.ref
        # If too much time has passed since the last heartbeat
        # then this curator instance is no longer active.
        if ((datetime.utcnow() - self._last_heartbeat_time).
                total_seconds() * 1000.0 > _curator_heartbeat_timeout_in_ms):
            if self._is_active:
                raise _CuratorNoLongerActive
            return False
        return True

    # Determine whether this instance is the active curator instance.
    def _determine_active_status(self):
        """
        Determine whether this curator is the active curator.
        :return: whether this curator is the active curator
        """
        if self._is_active:
            # Try to send a heartbeat and return the result indicating
            # whether this curator instance should continue to curate.
            return self._try_send_heartbeat()
        # If not active then check to see when the last active curator
        # heartbeat was sent.
        response = self._client.get(_curators_collection,
                _active_curator_key, None, False)
        try:
            response.raise_for_status()
        except Exception as e:
            # Indicates that no curator is active.
            if (_get_httperror_status_code(e) == 404):
                # Try to send a heartbeat and return the result indicating
                # whether this curator instance has become active.
                return self._try_send_heartbeat(add_new_record=True)
            else:
                raise e
        active_curator_details = _ActiveCuratorDetails(
                response.json['curator_id'],
                dateutil.parser.parse(response.json['timestamp']))
        # If the last active curator's heartbeat is timed out then
        # try to become the active curator.
        if ((datetime.utcnow() - active_curator_details.timestamp).
                total_seconds() * 1000.0 > _curator_heartbeat_timeout_in_ms):
            time.sleep(_additional_timeout_wait_in_ms / 1000.0)
            self._last_heartbeat_ref = response.ref
            self._last_heartbeat_time = active_curator_details.timestamp
            return self._try_send_heartbeat()
        else:
            return False

    def _curate(self):
        """
        Curate any broken jobs and locks in o.io.
        """
        was_something_curated = False
        pages = self._client.list(_jobs_collection)
        jobs = pages.all()
        for job in jobs:
            try:
                if job is None:
                    continue
                if ((datetime.utcnow() - dateutil.parser.parse(
                        job['value']['timestamp'])).total_seconds() * 1000.0 >
                        _max_job_time_in_ms + _additional_timeout_wait_in_ms):
                    was_something_curated = True
                    # Iterate on the journal items and roll back each one.
                    for item in job['value']['items']:
                        journal_item = _JournalItem(item['timestamp'],
                                item['collection'], item['key'],
                                item['original_value'],
                                item['new_value'])
                        _roll_back_journal_item(self._client, journal_item,
                                self._try_send_heartbeat)
                    self._append_to_removed_job_ids(job['path']['key'])
                    self._try_send_heartbeat()
                    response = self._client.delete(_jobs_collection,
                            job['path']['key'], None, False)
                    response.raise_for_status()
            except _CuratorNoLongerActive:
                raise
            # TODO: Change general exception catch to catch
            # specific exceptions.
            except Exception as e:
                print('Caught while processing a job: ' +
                      _format_exception(e))
        pages = self._client.list(_locks_collection)
        locks = pages.all()
        for lock in locks:
            try:
                if lock is None:
                    continue
                is_lock_associated_with_removed_job = (lock['value']['job_id']
                        in self._removed_job_ids)
                if (is_lock_associated_with_removed_job or
                        (datetime.utcnow() - dateutil.parser.parse(
                        lock['value']['job_timestamp'])).total_seconds() *
                        1000.0 > _max_job_time_in_ms +
                        _additional_timeout_wait_in_ms):
                    if is_lock_associated_with_removed_job is False:
                        response = self._client.get(_jobs_collection,
                                lock['value']['job_id'], None, False)
                        if response.status_code == 404:
                            is_lock_associated_with_removed_job = True
                    if is_lock_associated_with_removed_job:
                        was_something_curated = True
                        self._try_send_heartbeat()
                        response = self._client.delete(_locks_collection,
                                lock['path']['key'], lock['path']['ref'],
                                False)
                        response.raise_for_status()
            except _CuratorNoLongerActive:
                raise
            # TODO: Change general exception catch to catch
            # specific exceptions.
            except Exception as e:
                print('Caught while processing a lock: ' +
                      _format_exception(e))
        return was_something_curated

    def run(self):
        """
        Run this curator instance.
        """
        while (self._should_continue_to_run):
            try:
                if self._determine_active_status():
                    self._is_active = True
                    if self._curate() is False:
                        time.sleep(_curator_heartbeat_interval_in_ms
                                / 2.0 / 1000.0)
                    continue
            except _CuratorNoLongerActive:
                pass
            self._make_inactive_and_sleep()


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
