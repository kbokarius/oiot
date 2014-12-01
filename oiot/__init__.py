import os, sys, traceback
import json, random, string, datetime, uuid

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'
_curators_collection = 'oiot-curators'
_active_curator_key = 'active'
_curator_heartbeat_interval_in_ms = 500
_curator_heartbeat_timeout_in_ms = 5000
_curator_inactivity_delay_in_ms = 3000
_max_job_time_in_ms = 5000

def _generate_key():
	return ''.join(random.choice(string.ascii_uppercase + string.digits) 
		   for _ in range(16))	

def _get_lock_collection_key(collection_to_lock, key_to_lock):
	return collection_to_lock + "-" + key_to_lock

# Required for maintaining tracebacks for wrapped exceptions for Python
# 2.x / 3.x compatibility. 
# NOTE: Must be called immediately after the exception is caught.
def _format_exception(e):
	return (str(e) + ': ' +
		   traceback.format_exc(sys.exc_info()))

class _ActiveCuratorDetails(object):
	def __init__(self, curator_id = None, timestamp = None):
		self.curator_id = curator_id
		self.timestamp = timestamp

class _Lock(object):
	def __init__(self, job_id = None, timestamp = None, collection = None,
				 key = None, lock_ref = None):
		self.job_id = job_id
		self.timestamp = timestamp
		self.collection = collection
		self.key = key
		self.lock_ref = lock_ref

class _JournalItem(object):
	def __init__(self, timestamp = None, collection = None, key = None,
				 original_value = None, original_ref = None, 
				 new_value = None, new_ref = None):
		self.timestamp = timestamp
		self.collection = collection
		self.key = key
		self.original_value = original_value
		self.original_ref = original_ref
		self.new_value = new_value
		self.new_ref = new_ref

class _Encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.datetime):
			return obj.isoformat()
		elif isinstance(obj, _JournalItem):
			return vars(obj)
		elif isinstance(obj, uuid.UUID):
			return str(obj)
		return json.JSONEncoder.default(self, obj)

class CollectionKeyIsLocked(Exception):
	pass

class FailedToComplete(Exception):
	pass

class FailedToRollBack(Exception):
	pass

class RollbackCausedByException(Exception):
	pass

class JobIsFailed(Exception):
	pass

class JobIsCompleted(Exception):
	pass

class JobIsRolledBack(Exception):
	pass

class JobIsTimedOut(Exception):
	pass

from .client import OiotClient
from .job import Job
from .curator import Curator
