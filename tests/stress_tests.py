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
			  run_test_verify_operations_and_roll_back, \
			  run_test_exception_raised_when_key_locked
from subprocess import Popen
from datetime import datetime
import threading

class StressTests(unittest.TestCase):
	def _get_client(self):
		global _oio_api_key
		client = OiotClient(_oio_api_key)
		client.ping().raise_for_status()
		return client

	def setUp(self):
		self._minutes_to_run = 10
		self._curator_sleep_time_multiplier = 8
		self._number_of_curators = 2
		self._number_of_curator_test_threads_threads = 7
		self._number_of_job_test_threads = 2
		self._curator_threads = {}
		self._curator_thread_exception = None
		self._curator_tests_thread_exception = None
		self._job_tests_thread_exception = None
		#global _were_collections_cleared
		#if _were_collections_cleared is not True:
		#	_clear_test_collections(self._get_client())
		#	# Sleep to give o.io time to delete the collections. Without this
		#	# delay inconsistent results will be encountered.
		#	time.sleep(4)
		#	_were_collections_cleared = True

	def tearDown(self):
		self._should_run_curator_tests = False
		self._should_run_job_tests = False
		for thread in self._curator_threads:
			self._curator_threads[thread]._should_continue_to_run = False

	def run_curator(self, curator):
		try:
			curator.run()
		except Exception as e:			
			self._curator_thread_exception = _format_exception(e)

	def _run_curator_tests(self, index):
		try:
			client = self._get_client()
			while (self._should_run_curator_tests):
				print('Running curator test...')
				run_test_curation_of_timed_out_jobs(client, self)
				run_test_curation_of_timed_out_locks(client, self)
				run_test_changed_records_are_not_rolled_back(client, self)
			self._finished_curator_tests[index] = True
		except Exception as e:
			self._curator_tests_thread_exception = _format_exception(e)

	def _fail(self, failure_details):
		self._should_run_curator_tests = False
		self._should_run_job_tests = False
		for thread in self._curator_threads:
			self._curator_threads[thread]._should_continue_to_run = False
		self.fail(failure_details)

	def _run_job_tests(self, index):
		try:
			client = self._get_client()
			while (self._should_run_job_tests):
				print('Running job test...')
				run_test_job_timeout(client, self)
				run_test_basic_job_completion(client, self)
				run_test_basic_job_rollback(client, self)
				run_test_rollback_caused_by_exception(client, self)
				run_test_failed_completion(client, self)
				run_test_failed_rollback(client, self)		  
				run_test_job_and_lock_creation_and_removal(client, self)
				run_test_job_and_lock_creation_and_removal2(client, self)
				run_test_verify_operations_and_roll_back(client, self)
				run_test_exception_raised_when_key_locked(client, self)
			self._finished_job_tests[index] = True
		except Exception as e:
			self._job_tests_thread_exception = _format_exception(e)

	def test_one_curator_active_at_a_time(self):
		start_time = datetime.utcnow()
		client = self._get_client()
		for index in range(self._number_of_curators):
			print('Starting curator...')
			curator = Curator(client)
			thread = threading.Thread(target = self.run_curator,
					 args = (curator,))
			thread.start()
			self._curator_threads[thread] = curator
		time.sleep((_curator_inactivity_delay_in_ms * 2) / 1000.0)
		self._should_monitor_curator_threads = True
		self._should_run_curator_tests = True
		self._should_run_job_tests = True
		self._finished_curator_tests = []
		self._finished_job_tests = []
		for index in range(self._number_of_curator_test_threads_threads):
			time.sleep(3)
			self._finished_curator_tests.append(False)
			threading.Thread(target = self._run_curator_tests, 
							 args = (index,)).start()
		for index in range(self._number_of_job_test_threads):
			time.sleep(3)
			self._finished_job_tests.append(False)
			threading.Thread(target = self._run_job_tests, 
							 args = (index,)).start()
		while ((datetime.utcnow() - start_time).total_seconds() < 
				self._minutes_to_run * 60.0):
			if self._curator_thread_exception:
				self._fail(self._curator_thread_exception)
			if self._curator_tests_thread_exception:
				self._fail(self._curator_tests_thread_exception)
			if self._job_tests_thread_exception:
				self._fail(self._job_tests_thread_exception)
			time.sleep(5)
		print('Turning off test threads...')
		self._should_run_curator_tests = False
		self._should_run_job_tests = False
		print('Waiting for test threads to finished...')			
		all_test_group_threads_finished = False
		while (all_test_group_threads_finished is False):
			time.sleep(1)
			all_test_group_threads_finished = True
			for test_index in range(self._number_of_curator_test_threads_threads):
				if (self._finished_curator_tests[test_index]
						is False):
					all_test_group_threads_finished = False
			for test_index in range(self._number_of_job_test_threads):
				if (self._finished_job_tests[test_index] is False):
					all_test_group_threads_finished = False
		print('Test threads finished.')
		for thread in self._curator_threads:
			self._curator_threads[thread]._should_continue_to_run = False

if __name__ == '__main__':
	unittest.main()
