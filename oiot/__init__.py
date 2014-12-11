import os, sys, traceback, binascii
import json, random, string, datetime, uuid
from datetime import datetime

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'
_curators_collection = 'oiot-curators'
_active_curator_key = 'active'
_curator_heartbeat_interval_in_ms = 500
_curator_heartbeat_timeout_in_ms = 7500
_curator_inactivity_delay_in_ms = 3000
_max_job_time_in_ms = 5000
_additional_timeout_wait_in_ms = 1000
_deleted_object_value = {"deleted": "{A0981677-7933-4A5C-A141-9B40E60BD411}"}

def _generate_key():
	return str(binascii.b2a_hex(os.urandom(8)))

def _get_lock_collection_key(collection_to_lock, key_to_lock):
	return collection_to_lock + "-" + str(key_to_lock)

def _get_httperror_status_code(exception):
	if exception.__class__.__name__ is 'HTTPError':
		return exception.response.status_code
	else:
		return None

def _format_exception(e):
	return traceback.format_exc()

def _create_and_add_lock(client, collection, key, job_id, timestamp):
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
	def __init__(self, curator_id = None, timestamp = None):
		self.curator_id = curator_id
		self.timestamp = timestamp

class _Lock(object):
	def __init__(self, job_id = None, job_timestamp = None, timestamp = None,
				collection = None, key = None, lock_ref = None):
		self.job_id = job_id
		self.job_timestamp = job_timestamp
		self.timestamp = timestamp
		self.collection = collection
		self.key = key
		self.lock_ref = lock_ref

class _JournalItem(object):
	def __init__(self, timestamp = None, collection = None, key = None,
				original_value = None, new_value = None):
		self.timestamp = timestamp
		self.collection = collection
		self.key = key
		self.original_value = original_value
		self.new_value = new_value

class _Encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime):
			return obj.isoformat()
		elif isinstance(obj, _JournalItem):
			return vars(obj)
		elif isinstance(obj, uuid.UUID):
			return str(obj)
		return json.JSONEncoder.default(self, obj)

class CollectionKeyIsLocked(Exception):
	pass

class FailedToComplete(Exception):
	def __init__(self, exception_failing_completion=None,
				stacktrace_failing_completion=None):
		super(FailedToComplete, self).__init__()
		self.exception_failing_completion = exception_failing_completion
		self.stacktrace_failing_completion = stacktrace_failing_completion

class FailedToRollBack(Exception):
	def __init__(self, exception_causing_rollback=None,
				stacktrace_causing_rollback=None,
				exception_failing_rollback=None,
				stacktrace_failing_rollback=None):
		super(FailedToRollBack, self).__init__()
		self.exception_causing_rollback = exception_causing_rollback
		self.stacktrace_causing_rollback = stacktrace_causing_rollback
		self.exception_failing_rollback = exception_failing_rollback
		self.stacktrace_failing_rollback = stacktrace_failing_rollback

class RollbackCausedByException(Exception):
	def __init__(self, exception_causing_rollback=None,
				stacktrace_causing_rollback=None):
		super(RollbackCausedByException, self).__init__()
		self.exception_causing_rollback = exception_causing_rollback
		self.stacktrace_causing_rollback = stacktrace_causing_rollback

class JobIsFailed(Exception):
	pass

class JobIsCompleted(Exception):
	pass

class JobIsRolledBack(Exception):
	pass

class JobIsTimedOut(Exception):
	pass

class CuratorNoLongerActive(Exception):
	pass

from .client import OiotClient
from .job import Job
from .curator import Curator
