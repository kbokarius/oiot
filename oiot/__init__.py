import os, sys, traceback, binascii
import json, random, string, datetime, uuid

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'
_curators_collection = 'oiot-curators'
_active_curator_key = 'active'
_curator_heartbeat_interval_in_ms = 500
_curator_heartbeat_timeout_in_ms = 10000
_curator_inactivity_delay_in_ms = 3000
_max_job_time_in_ms = 5000
_additional_timeout_wait_in_ms = 1000

def _generate_key():
	return binascii.b2a_hex(os.urandom(8))

def _get_lock_collection_key(collection_to_lock, key_to_lock):
	return collection_to_lock + "-" + key_to_lock

def _get_httperror_status_code(exception):
	if exception.__class__.__name__ is 'HTTPError':
		return exception.response.status_code
	else:
		return None

# Required for maintaining tracebacks for wrapped exceptions for Python
# 2.x / 3.x compatibility. 
# NOTE: Must be called immediately after the exception is caught.
def _format_exception(e):
	return (str(e) + ': ' +
		   traceback.format_exc(sys.exc_info()))

def _roll_back_journal_item(client, journal_item, raise_if_timed_out):
	# Don't attempt to roll-back if the original value and the
	# new value are the same.
	if journal_item.original_value == journal_item.new_value:
		return
	raise_if_timed_out()
	get_response = client.get(journal_item.collection,
			   journal_item.key, None, False)
	try: 
		get_response.raise_for_status()
	except Exception as e:
		if (_get_httperror_status_code(e) == 404):
			return
		else:
			raise e
	# Don't attempt to roll-back if the new value does not match
	# or if the record has been deleted.
	if get_response.json != journal_item.new_value:
		return
	# Was there an original value?
	if journal_item.original_value:
		# Put back the original value only if the new
		# value matches.
		if get_response.json == journal_item.new_value:
			raise_if_timed_out()
			try: 
				put_response = client.put(
						   journal_item.collection,
						   journal_item.key, 
						   journal_item.original_value, 
						   get_response.ref, False)
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

class CuratorNoLongerActive(Exception):
	pass

from .client import OiotClient
from .job import Job
from .curator import Curator
