from porc import Client
from . import _locks_collection, _get_lock_collection_key, \
			  _curators_collection, _active_curator_key, \
			  _curator_heartbeat_timeout_in_ms, _Encoder, \
			  _curator_inactivity_delay_in_ms, _ActiveCuratorDetails, \
			  _curator_heartbeat_interval_in_ms, _get_httperror_status_code, \
			  _jobs_collection, _format_exception, _JournalItem, _Lock, \
			  _max_job_time_in_ms, _roll_back_journal_item, \
			  _locks_collection

# TODO: Log unexpected exceptions locally and to 'oiot-errors'

from datetime import datetime
import dateutil.parser
import uuid, time, json

class Curator(Client):
	def __init__(self, client):
		self._client = client
		self._id = uuid.uuid4()
		self._is_active = False
		self._last_heartbeat_time = None
		self._last_heartbeat_ref = None

	def _remove_locks_and_job(self, job_id):
		pages = self._client.list(_locks_collection)
		locks = pages.all()
		for item in locks:
			try:
				lock = _Lock(item['value']['job_id'],
							 item['value']['timestamp'],
							 item['value']['collection'],
							 item['value']['key'],
							 item['value']['lock_ref'])
				if lock.job_id == job_id:
					response = self._client.delete(_locks_collection,
							  _get_lock_collection_key(lock.collection,
							  lock.key), lock.lock_ref, False)
					response.raise_for_status()
			# TODO: Change general exception catch to catch 
			# specific exceptions.
			except Exception as e:
				print('Caught while removing a lock associated with a job: ' +
					  _format_exception(e))
		try:
			response = self._client.delete(_jobs_collection, job_id, 
					   None, False)
			response.raise_for_status()
		# TODO: Change general exception catch to catch 
		# specific exceptions.
		except Exception as e:
			print('Caught while removing job: ' + _format_exception(e))

	def _make_inactive_and_sleep(self):
		self._is_active = False
		time.sleep(_curator_inactivity_delay_in_ms / 1000.0)

	def _try_send_heartbeat(self, add_new_record = False):
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
				return False
			else:
				raise e
		self._last_heartbeat_time = active_curator_details.timestamp
		self._last_heartbeat_ref = response.ref
		return True

	# Determine whether this instance is the active curator instance.
	def _determine_active_status(self):
		if self._is_active:
			# If too little time has passed since the last heartbeat 
			# then don't try to send another heartbeat.
			if ((datetime.utcnow() - self._last_heartbeat_time).
					total_seconds() * 1000.0 < _curator_heartbeat_interval_in_ms):
				return True
			# If too much time has passed since the last heartbeat
			# then this curator instance is no longer active.
			if ((datetime.utcnow() - self._last_heartbeat_time).
					total_seconds() * 1000.0 > _curator_heartbeat_timeout_in_ms):
				return False
			# Try to send a heartbeat and return the result indicating
			# whether this curator instance should continue to curate.
			return self._try_send_heartbeat()
		# Check to see when the last active curator heartbeat was sent.
		response = self._client.get(_curators_collection,
									_active_curator_key, None, False)
		try:
			response.raise_for_status()
		except Exception as e:
			# Indicates that no curator is active.
			if (_get_httperror_status_code(e) == 404):
				# Try to send a heartbeat and return the result indicating
				# whether this curator instance has become active.
				return self._try_send_heartbeat(True)
			else:
				raise e
		active_curator_details = _ActiveCuratorDetails(
						response.json['curator_id'],
						dateutil.parser.parse(response.json['timestamp']))
		# If the last active curator's heartbeat is timed out then
		# try to become the active curator.
		if ((datetime.utcnow() - active_curator_details.timestamp).
				total_seconds() * 1000.0 > _curator_heartbeat_timeout_in_ms):
			# Wait a second to mitigate the potential of a race condition
			# between the last active curator instance and this instance.
			time.sleep(1)
			self._last_heartbeat_ref = response.ref
			self._last_heartbeat_time = active_curator_details.timestamp
			return self._try_send_heartbeat()
		else:
			return False

	def _curate(self):
		was_something_curated = False
		pages = self._client.list(_jobs_collection)
		jobs = pages.all()
		for job in jobs:
			try:
				# Add a second to the max job time to mitigate the potential
				# for race conditions.
				if ((datetime.utcnow() - dateutil.parser.parse(
						job['value']['timestamp'])).total_seconds() * 1000.0 >
						_max_job_time_in_ms + 1000):
					# Iterate on the journal items and roll back each one.
					for item in job['value']['items']:
						journal_item = _JournalItem(item['timestamp'],
									   item['collection'], item['key'],
									   item['original_value'],
									   item['new_value'])
						_roll_back_journal_item(self._client, journal_item)
					# Remove all locks associated with the job
					# and the job itself.
					self._remove_locks_and_job(job['path']['key'])
					was_something_curated = True
			# TODO: Change general exception catch to catch 
			# specific exceptions.
			except Exception as e:
				print('Caught while processing a job: ' +
					  _format_exception(e))
		# TODO: Process locks.
		return was_something_curated	

	def run(self):
		while (True):
			if self._determine_active_status():
				self._is_active = True
				if self._curate() is False:
					time.sleep(_curator_heartbeat_interval_in_ms / 1000.0)
			else:
				self._make_inactive_and_sleep()
