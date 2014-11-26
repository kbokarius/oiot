import os, sys
import json, random, string, datetime

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'

def _generate_key():
	return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))	

def _get_lock_collection_key(collection_to_lock, key_to_lock):
	return collection_to_lock + "-" + key_to_lock

class CollectionKeyIsLocked(Exception):
	pass

class JobIsCompleted(Exception):
	pass

class JobIsRolledBack(Exception):
	pass

class _Encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.datetime):
			return obj.isoformat()
		return json.JSONEncoder.default(self, obj)

class _Lock:
	job_id = None
	timestamp = None
	collection = None
	key = None
	ref = None
	lock_ref = None

class _JobItem:
	job_item_type = None
	collection = None
	key = None
	value = None
	original_ref = None
	response = None
	is_completed = False

from .client import OiotClient
from .job import Job
