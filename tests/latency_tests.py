import os, sys, unittest, time, dateutil, json
from datetime import datetime
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut, \
				 _get_lock_collection_key, _get_httperror_status_code, \
				 _jobs_collection
from . import _were_collections_cleared, _oio_api_key, \
			  _verify_job_creation, _clear_test_collections, \
			  _verify_lock_creation
import threading

_threads = []
_averages = []

def _get_client():
	global _oio_api_key
	client = OiotClient(_oio_api_key)
	client.ping().raise_for_status()
	return client

def _run_latency_test():
	client = _get_client()
	execution_times = []
	start_time = datetime.utcnow()
	while ((datetime.utcnow() - start_time).total_seconds() < 35):		
		op_start_time = time.time()
		response = client.post('test2', {'value_key2': 'value2'})
		response.raise_for_status()
		client.get('test2', response.key).raise_for_status()
		client.put('test2', response.key, 
				  {'value_newkey2': 'newvalue2'}).raise_for_status()
		execution_times.append(time.time() - op_start_time)
	_averages.append(reduce(lambda x, y: x + y, 
			execution_times) / len(execution_times))
	
def _run_latency_test_using_job():
	client = _get_client()
	execution_times = []
	start_time = datetime.utcnow()
	while ((datetime.utcnow() - start_time).total_seconds() < 35):		
		op_start_time = time.time()
		job = Job(client)
		response = job.post('test2', {'value_key2': 'value2'})
		response.raise_for_status()
		client.get('test2', response.key, raise_if_locked=False).raise_for_status()
		job.put('test2', response.key, 
				  {'value_newkey2': 'newvalue2'}).raise_for_status()
		job.complete()
		execution_times.append(time.time() - op_start_time)
	_averages.append(reduce(lambda x, y: x + y, 
			execution_times) / len(execution_times))
	
if __name__ == '__main__':
	for index in range(65, 100, 8):
		for thread_index in range(index):
			thread = threading.Thread(target = _run_latency_test_using_job)
			_threads.append(thread)
			thread.start()		
		threads_are_running = True
		while threads_are_running:
			time.sleep(2)
			threads_are_running = False
			for thread in _threads:
				if thread.isAlive():
					threads_are_running = True
					break	
		print(str(index) + ' thread average: ' + str(reduce(lambda x, y: x + y, 
				_averages) / len(_averages)))
		_averages = []		
	
