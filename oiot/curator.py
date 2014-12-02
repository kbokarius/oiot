from porc import Client
from . import _locks_collection, _get_lock_collection_key, \
			  _curators_collection, _active_curator_key, \
			  _curator_heartbeat_timeout_in_ms, _Encoder, \
			  _curator_inactivity_delay_in_ms, _ActiveCuratorDetails, \
			  _curator_heartbeat_interval_in_ms

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
				   cls=_Encoder)), last_ref_value)
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
									_active_curator_key, None)
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
		time.sleep(1)

	def run(self):
		while (True):
			if self._determine_active_status():
				self._is_active = True
				if self._curate() is False:
					time.sleep(_curator_heartbeat_interval_in_ms / 1000.0)
			else:
				self._make_inactive_and_sleep()
