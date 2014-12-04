import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut, \
				 Job, _curator_heartbeat_timeout_in_ms, \
				_curator_inactivity_delay_in_ms, _get_lock_collection_key, \
				Curator, _format_exception
from . import _were_collections_cleared, _oio_api_key, \
			  _verify_job_creation, _clear_test_collections, \
			  _verify_lock_creation, run_test_curation_of_timed_out_jobs, \
			  run_test_curation_of_timed_out_locks, run_test_job_timeout, \
			  run_test_changed_records_are_not_rolled_back, \
			  run_test_basic_job_completion, run_test_basic_job_rollback, \
			  run_test_rollback_caused_by_exception, \
			  run_test_failed_completion, run_test_failed_rollback, \
			  run_test_job_and_lock_creation_and_removal, \
			  run_test_job_and_lock_creation_and_removal2, \
			  run_test_verify_writes_and_roll_back
from subprocess import Popen
from datetime import datetime
import threading

# TODO: Ensure tests failures on background threads cause
# the main thread tests to fail and don't hang up the process.

class StressTests(unittest.TestCase):
	def _get_client(self):
		global _oio_api_key
		client = OiotClient(_oio_api_key)
		client.ping().raise_for_status()
		return client

	def setUp(self):
		self._curator_sleep_time_multiplier = 1.5
		self._curator_threads = {}
		self._monitor_curator_threads_exception = None
		#global _were_collections_cleared
		#if _were_collections_cleared is not True:
		#	_clear_test_collections(self._get_client())
		#	# Sleep to give o.io time to delete the collections. Without this
		#	# delay inconsistent results will be encountered.
		#	time.sleep(4)
		#	_were_collections_cleared = True

	def tearDown(self):
		self._should_monitor_curator_threads = False
		for thread in self._curator_threads:
			self._curator_threads[thread]._should_continue_to_run = False

	def run_curator(self, curator):
		curator.run()

	def _run_curator_tests(self, index):
		client = self._get_client()
		while (self._should_run_curator_tests):
			print 'running curator test'
			run_test_curation_of_timed_out_jobs(client, self)
			run_test_curation_of_timed_out_locks(client, self)
			run_test_changed_records_are_not_rolled_back(client, self)
		self._finished_curator_tests[index] = True

	def _run_job_tests(self, index):
		client = self._get_client()
		while (False): #(self._should_run_job_tests):
			print 'running job test'
			run_test_job_timeout(client, self)
			run_test_basic_job_completion(client, self)
			run_test_basic_job_rollback(client, self)
			run_test_rollback_caused_by_exception(client, self)
			run_test_failed_completion(client, self)
			run_test_failed_rollback(client, self)		  
			run_test_job_and_lock_creation_and_removal(client, self)
			run_test_job_and_lock_creation_and_removal2(client, self)
			run_test_verify_writes_and_roll_back(client, self)
		self._finished_job_tests[index] = True

	def _monitor_curator_threads(self):
		try:
			no_active_curator_timestamp = None
			while (self._should_monitor_curator_threads):
				active_curator = None
				for thread in self._curator_threads:
					if (self._curator_threads[thread]._is_active):
						active_curator = self._curator_threads[thread]
				if active_curator is not None:				
					no_active_curator_timestamp = None	
				elif (no_active_curator_timestamp is not None and 
						active_curator == None):
					self.assertTrue((datetime.utcnow() - 
							no_active_curator_timestamp).total_seconds() < 
							_curator_inactivity_delay_in_ms / 1000.0 * 3)
				else:
					no_active_curator_timestamp = datetime.utcnow()
				time.sleep(0.05)
		except Exception as e:
			self._monitor_curator_threads_exception = _format_exception(e)

	def test_one_curator_active_at_a_time(self):
		client = self._get_client()
		number_of_curators = 2
		for index in range(number_of_curators):
			print 'starting curator'
			curator = Curator(client)
			thread = threading.Thread(target = self.run_curator,
					 args = (curator,))
			thread.start()
			self._curator_threads[thread] = curator
		time.sleep((_curator_inactivity_delay_in_ms * 2) / 1000.0)
		self._should_monitor_curator_threads = True
		self._should_run_curator_tests = True
		self._should_run_job_tests = True
		#threading.Thread(target = self._monitor_curator_threads).start()
		number_of_threads_per_test_group = 3
		self._finished_curator_tests = []
		self._finished_job_tests = []
		for index in range(number_of_threads_per_test_group):
			self._finished_curator_tests.append(False)
			self._finished_job_tests.append(False)
			threading.Thread(target = self._run_curator_tests, 
							 args = (index,)).start()
			threading.Thread(target = self._run_job_tests, 
							 args = (index,)).start()
		index = 0
		for thread in self._curator_threads:
			if self._monitor_curator_threads_exception:
				self.fail(self._monitor_curator_threads_exception)
			time.sleep(_curator_inactivity_delay_in_ms / 1000.0 * 1000)
			if index == number_of_curators - 1:
				print 'turning off test threads'
				self._should_run_curator_tests = False
				self._should_run_job_tests = False
				print 'final step'			
				all_test_group_threads_finished = False
				while (all_test_group_threads_finished is False):
					time.sleep(1)
					all_test_group_threads_finished = True
					for test_group in range(number_of_threads_per_test_group):
						if (self._finished_curator_tests[test_group] is False or
								self._finished_job_tests[test_group] is False):
							all_test_group_threads_finished = False
				print 'finished final step'
			self._curator_threads[thread]._should_continue_to_run = False
			self._curator_threads[thread]._is_active = False
			print 'killed: ' + str(index)
			index += 1
		self._should_monitor_curator_threads = False

if __name__ == '__main__':
	unittest.main()