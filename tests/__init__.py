from oiot import _locks_collection, _jobs_collection, _curators_collection, \
				 _get_lock_collection_key
from datetime import datetime
import dateutil

_were_collections_cleared = False
_oio_api_key = '69b4329e-990e-4969-b0ec-b7ef680fd32b'

def _clear_test_collections(client):
	client.delete('test1')
	client.delete('test2')
	client.delete(_locks_collection)
	client.delete(_jobs_collection)
	client.delete(_curators_collection)

def _verify_job_creation(testinstance, job):
	response = job._client.get(_jobs_collection, job._job_id,
			   None, False)
	response.raise_for_status()
	testinstance.assertTrue((datetime.utcnow() - 
					 dateutil.parser.parse(
					 response.json['timestamp'])).
					 total_seconds() < 2.0)
	testinstance.assertTrue('items' in response.json)

def _verify_lock_creation(testinstance, job, collection, key):
	response = job._client.get(_locks_collection, 
			   _get_lock_collection_key(collection, key), 
			   None, False)
	response.raise_for_status()
	testinstance.assertEqual(response.json['job_id'], job._job_id)
	testinstance.assertEqual(response.json['collection'], collection)
	testinstance.assertEqual(response.json['key'], key)

from .curator_tests import run_test_curation_of_timed_out_jobs, \
		run_test_curation_of_timed_out_locks, \
		run_test_changed_records_are_not_rolled_back
from .job_tests import run_test_basic_job_completion, \
		run_test_basic_job_rollback, run_test_rollback_caused_by_exception, \
		run_test_failed_completion, run_test_failed_rollback, \
		run_test_job_timeout, run_test_job_and_lock_creation_and_removal, \
		run_test_job_and_lock_creation_and_removal2, \
		run_test_verify_writes_and_roll_back
