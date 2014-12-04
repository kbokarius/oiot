from porc import Client
from . import _locks_collection, _get_lock_collection_key, \
			  _curators_collection, _active_curator_key, \
			  _curator_heartbeat_timeout_in_ms, _Encoder, \
			  _curator_inactivity_delay_in_ms, _ActiveCuratorDetails, \
			  _curator_heartbeat_interval_in_ms, _get_httperror_status_code, \
			  _jobs_collection, _format_exception, _JournalItem, _Lock, \
			  _max_job_time_in_ms, _roll_back_journal_item, \
			  _locks_collection, _additional_timeout_wait_in_ms, \
			  CuratorIsTimedOut

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
		self._should_continue_to_run = True

	def _remove_locks(self, job_id):
		pages = self._client.list(_locks_collection)
		locks = pages.all()
		for item in locks:
			self._raise_if_curator_is_timed_out()
			try:
				lock = _Lock(item['value']['job_id'],
							 item['value']['job_timestamp'],
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

	def _remove_job(self, job_id):
		self._raise_if_curator_is_timed_out()
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

	def _raise_if_curator_is_timed_out(self):
		if ((datetime.utcnow() - self._last_heartbeat_time).
				total_seconds() * 1000.0 > _curator_heartbeat_timeout_in_ms):
			raise CuratorIsTimedOut

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
			time.sleep(_additional_timeout_wait_in_ms / 1000.0)
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
			self._try_send_heartbeat()
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
										self._raise_if_curator_is_timed_out)
					# Remove all locks associated with the job
					# and the job itself.
					self._remove_locks(job['path']['key'])
					self._remove_job(job['path']['key'])
			except CuratorIsTimedOut:
				raise
			# TODO: Change general exception catch to catch 
			# specific exceptions.
			except Exception as e:
				print('Caught while processing a job: ' +
					  _format_exception(e))
		pages = self._client.list(_locks_collection)
		locks = pages.all()
		for lock in locks:
			self._try_send_heartbeat()
			try:
				if ((datetime.utcnow() - dateutil.parser.parse(
						lock['value']['job_timestamp'])).total_seconds() * 
						1000.0 > _max_job_time_in_ms + 
						_additional_timeout_wait_in_ms):
					self._raise_if_curator_is_timed_out()
					response = self._client.get(_jobs_collection, 
									 lock['value']['job_id'], None, False)
					# Only remove job-less locks. Otherwise allow the job
					# processing mechanism above to clean up the job.
					if response.status_code == 404:
						was_something_curated = True
						self._remove_locks(lock['value']['job_id'])
			except CuratorIsTimedOut:
				raise
			# TODO: Change general exception catch to catch 
			# specific exceptions.
			except Exception as e:
				print('Caught while processing a lock: ' +
					  _format_exception(e))
		return was_something_curated

	def run(self):
		while (self._should_continue_to_run):
			if self._determine_active_status():
				self._is_active = True
				if self._curate() is False:
					time.sleep(_curator_heartbeat_interval_in_ms / 1000.0)
			else:
				self._make_inactive_and_sleep()
