# collection name to use for the locks collection
_locks_collection = 'oiot-locks'

# collection name to use for the jobs collection
_jobs_collection = 'oiot-jobs'

# collection name to use for the curators collection
_curators_collection = 'oiot-curators'

# collection key name to use for the active curator
_active_curator_key = 'active'

# minimum heartbeat interval for curators
_curator_heartbeat_interval_in_ms = 500

# elapsed time before a curator times out and another should take its place
_curator_heartbeat_timeout_in_ms = 7500

# time an inactive curator should sleep between status checks
_curator_inactivity_delay_in_ms = 3000

# elapsed time before a job is timed out and automatically rolled back
_max_job_time_in_ms = 5000

# additional elapsed time used by active curators before rolling back jobs
_additional_timeout_wait_in_ms = 1000

# value used by journal items to indicate a delete operation was performed
_deleted_object_value = {"deleted": "{A0981677-7933-4A5C-A141-9B40E60BD411}"}
