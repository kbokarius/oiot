import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut, \
				 Job, _curator_heartbeat_timeout_in_ms, \
				_curator_inactivity_delay_in_ms, _get_lock_collection_key
from . import _were_collections_cleared, _oio_api_key, \
			  _verify_job_creation, _clear_test_collections, \
			  _verify_lock_creation
from subprocess import Popen

class CuratorTests(unittest.TestCase):
	def setUp(self):
		# Verify o.io is up and the key is valid.
		global _oio_api_key
		self._client = OiotClient(_oio_api_key)
		self._client.ping().raise_for_status()
		global _were_collections_cleared
		if _were_collections_cleared is not True:
			_clear_test_collections(self._client)
			# Sleep to give o.io time to delete the collections. Without this
			# delay inconsistent results will be encountered.
			time.sleep(4)
			_were_collections_cleared = True
		self._curator_process = Popen(['python', 'run_curator.py', 
								_oio_api_key])

	def tearDown(self):
		self._curator_process.kill()

	def test_curation_of_timed_out_jobs(self):		
		response3 = self._client.put('test3', 'test3-key-636',
					{'value_key3': 'value_value3'})
		response3.raise_for_status()
		response = self._client.get('test3', 'test3-key-636', 
				   response3.ref, False)
		response.raise_for_status()
		self.assertEqual({'value_key3': 'value_value3'}, response.json)
		job = Job(self._client)
		response2 = job.post('test2', {'value_key2': 'value_value2'})
		response2.raise_for_status()
		response = self._client.get('test2', response2.key,
				   response2.ref, False)
		response.raise_for_status()
		self.assertEqual({'value_key2': 'value_value2'}, response.json)
		response3 = job.put('test3', 'test3-key-636',
					{'value_newkey3': 'value_newvalue3'}, response3.ref)
		response3.raise_for_status()
		response = self._client.get('test3', 'test3-key-636', 
				   response3.ref, False)
		response.raise_for_status()
		self.assertEqual({'value_newkey3': 'value_newvalue3'}, response.json)
		time.sleep(3 + (_curator_inactivity_delay_in_ms / 1000.0) +
				  (_curator_heartbeat_timeout_in_ms / 1000.0))
		response = self._client.get('test2', response2.key,
				   None, False)
		self.assertEqual(response.status_code, 404)
		response = self._client.get('test3', 'test3-key-636', 
				   None, False)
		response.raise_for_status()
		self.assertEqual({'value_key3': 'value_value3'}, response.json)
		response = self._client.get(_jobs_collection, job._job_id, 
				   None, False)
		self.assertEqual(response.status_code, 404)
		for lock in job._locks:
			if lock.job_id == job._job_id:
				response = self._client.get(_locks_collection,
						  _get_lock_collection_key(lock.collection,
						  lock.key), None, False)
				self.assertEqual(response.status_code, 404)

	def test_curation_of_timed_out_locks(self):		
		job = Job(self._client)
		job._get_lock('test2', 'test2-key-736', None)
		job._get_lock('test3', 'test3-key-736', None)
		for lock in job._locks:
			if lock.job_id == job._job_id:
				response = self._client.get(_locks_collection,
						  _get_lock_collection_key(lock.collection,
						  lock.key), None, False)
				response.raise_for_status()
		time.sleep(3 + (_curator_inactivity_delay_in_ms / 1000.0) +
				  (_curator_heartbeat_timeout_in_ms / 1000.0))
		for lock in job._locks:
			if lock.job_id == job._job_id:
				response = self._client.get(_locks_collection,
						  _get_lock_collection_key(lock.collection,
						  lock.key), None, False)
				self.assertEqual(response.status_code, 404)

	# TODO: Test to make sure a record's whose new value does not match
	# is not rolled-back. 
	# TODO: Test to make sure only one curator is active at any given time.
	# TODO: Add a stress-type tests that bangs on the system and simuates
	# jobs breaking.

if __name__ == '__main__':
	unittest.main()
