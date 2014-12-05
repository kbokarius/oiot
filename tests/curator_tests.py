import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut, \
				 Job, _curator_heartbeat_timeout_in_ms, \
				 _additional_timeout_wait_in_ms, _get_lock_collection_key, \
				 Curator, _generate_key, _max_job_time_in_ms
from . import _were_collections_cleared, _oio_api_key, \
			  _verify_job_creation, _clear_test_collections, \
			  _verify_lock_creation
from subprocess import Popen
import threading

def run_test_curation_of_timed_out_jobs(client, test_instance):
	test3_key = _generate_key()		
	response3 = client.put('test3', test3_key,
				{'value_key3': 'value_value3'})
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	job = Job(client)
	response2 = job.post('test2', {'value_key2': 'value_value2'})
	response2.raise_for_status()
	response = client.get('test2', response2.key,
			   response2.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key2': 'value_value2'}, response.json)
	response3 = job.put('test3', test3_key,
				{'value_newkey3': 'value_newvalue3'}, response3.ref)
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_newkey3': 'value_newvalue3'},
							  response.json)
	time.sleep(((_max_job_time_in_ms + _additional_timeout_wait_in_ms)
				 / 1000.0) * test_instance._curator_sleep_time_multiplier)
	response = client.get('test2', response2.key,
			   None, False)
	test_instance.assertEqual(response.status_code, 404)
	response = client.get('test3', test3_key, 
			   None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	response = client.get(_jobs_collection, job._job_id, 
			   None, False)
	test_instance.assertEqual(response.status_code, 404)
	for lock in job._locks:
		if lock.job_id == job._job_id:
			response = client.get(_locks_collection,
					  _get_lock_collection_key(lock.collection,
					  lock.key), None, False)
			test_instance.assertEqual(response.status_code, 404)

def run_test_curation_of_timed_out_locks(client, test_instance):
	test2_key = _generate_key()
	test3_key = _generate_key()
	job = Job(client)
	job._get_lock('test2', test2_key, None)
	job._get_lock('test3', test3_key, None)
	for lock in job._locks:
		if lock.job_id == job._job_id:
			response = client.get(_locks_collection,
					  _get_lock_collection_key(lock.collection,
					  lock.key), None, False)
			response.raise_for_status()
	time.sleep(((_max_job_time_in_ms + _additional_timeout_wait_in_ms)
				 / 1000.0) * test_instance._curator_sleep_time_multiplier)
	for lock in job._locks:
		if lock.job_id == job._job_id:
			response = client.get(_locks_collection,
					  _get_lock_collection_key(lock.collection,
					  lock.key), None, False)
			test_instance.assertEqual(response.status_code, 404)

def run_test_changed_records_are_not_rolled_back(client, test_instance):
	test3_key = _generate_key()
	response3 = client.put('test3', test3_key,
				{'value_key3': 'value_value3'})
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	job = Job(client)
	response2 = job.post('test2', {'value_key2': 'value_value2'})
	response2.raise_for_status()
	response = client.get('test2', response2.key,
			   response2.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key2': 'value_value2'}, response.json)
	response2 = client.put('test2', response2.key,
				{'value_changedkey2': 'value_changedvalue2'},
				response2.ref, False)
	response2.raise_for_status()
	response3 = job.put('test3', test3_key,
				{'value_newkey3': 'value_newvalue3'}, response3.ref)
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_newkey3': 'value_newvalue3'},
							   response.json)
	response3 = client.put('test3', test3_key,
				{'value_changedkey3': 'value_changedvalue3'},
				response3.ref, False)
	response3.raise_for_status()
	time.sleep(((_max_job_time_in_ms + _additional_timeout_wait_in_ms)
				 / 1000.0) * test_instance._curator_sleep_time_multiplier)
	response = client.get('test2', response2.key,
			   None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_changedkey2': 'value_changedvalue2'},
					 response.json)
	response = client.get('test3', test3_key, 
			   None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_changedkey3': 'value_changedvalue3'},
					 response.json)
	response = client.get(_jobs_collection, job._job_id,
			   None, False)
	test_instance.assertEqual(response.status_code, 404)
	for lock in job._locks:
		if lock.job_id == job._job_id:
			response = client.get(_locks_collection,
					  _get_lock_collection_key(lock.collection,
					  lock.key), None, False)
			test_instance.assertEqual(response.status_code, 404)

class CuratorTests(unittest.TestCase):
	def setUp(self):
		# Verify o.io is up and the key is valid.
		global _oio_api_key
		self._client = OiotClient(_oio_api_key)
		self._client.ping().raise_for_status()
		self._curator_sleep_time_multiplier = 2
		global _were_collections_cleared
		if _were_collections_cleared is not True:
			_clear_test_collections(self._client)
			# Sleep to give o.io time to delete the collections. Without this
			# delay inconsistent results will be encountered.
			time.sleep(4)
			_were_collections_cleared = True
		self._curator_processes = []
		# Start many curator processes to simulate a real environment.
		for index in range(5):
			self._curator_processes.append(Popen(['python', 'run_curator.py',
									_oio_api_key]))

	def tearDown(self):
		for process in self._curator_processes:
			process.kill()

	def test_curation_of_timed_out_jobs(self):
		run_test_curation_of_timed_out_jobs(self._client, self)

	def test_curation_of_timed_out_locks(self):
		run_test_curation_of_timed_out_locks(self._client, self)

	def test_changed_records_are_not_rolled_back(self):
		run_test_changed_records_are_not_rolled_back(self._client, self)

if __name__ == '__main__':
	unittest.main()
