import os, sys, traceback
import json, random, string, datetime

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'
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

class _Lock:
	job_id = None
	timestamp = None
	collection = None
	key = None
	lock_ref = None

class _JournalItem:
	timestamp = None
	collection = None
	key = None
	original_value = None
	original_ref = None
	new_value = None
	new_ref = None
	response = None

class _Encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.datetime):
			return obj.isoformat()
		elif isinstance(obj, _JournalItem):
			return vars(obj)
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
