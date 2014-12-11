## Overview 

A tool providing read/write locks and facilitating transactions for Orchestrate.io (o.io). Consists of a Job class allowing for multiple o.io operations to be executed as an atomic transaction, an OiotClient class inherited from porc.Client that raises exceptions if an attempt is made to access a locked collection key, and a Curator class that's designed to run as a service and clean up broken transactions. 

Note that using oiot requires additional o.io operations for the journaling and locking mechanisms, and subsequently introduces additional overhead when used instead of porc.Client. Therefore, oiot should only be used in conjunction with collections that require locks and/or transactions.

## Client

The OiotClient class inherits from porc.Client and overrides the methods that need to lock the specified collection keys prior to executing the corresponding o.io operations. The methods currently overridden are put(), get(), and delete(). If a collection key is locked when one of those methods is called then the CollectionKeyIsLocked exception will be raised. Note that "raise_if_locked=False" can be passed to these methods to ignore existing locks and revert to the standard porc.Client behavior. Since OiotClient not only provides the same methods as porc.Client but also maintains the same contracts, integrating oiot into an existing application is as easy as changing "client = porc.Client(API_KEY)" to "client = oiot.OiotClient(API_KEY)" and using the Job class whenever transactions are required.

## Jobs

Jobs utilize journaling and locking mechanisms where both mechanisms execute under the covers to ease consumption and use. Jobs currently support the get(), post(), put(), and delete() operations. Executing any of these operations through a job will result in the collection key being locked for the lifetime of the job. In order to finish a job it must be explicitly completed by calling the complete() method or explicitly rolled back by calling the roll-back() method. Jobs have a maximum lifetime determined by the _max_job_time_in_ms configuration setting and if that lifetime is exceeded at the time of an operation then the job will fail and automatically be rolled back.

Once all operations are executed via a job instance then the complete() method should be called to indicate that the job is complete. If a job fails to complete for any reason then a FailedToComplete custom exception is thrown including exception_failing_completion and stacktrace_failing_completion fields that contain the exception and stacktrace that caused the job completion to fail. Completing a job removes the job, the job's journal, and all locks associated with the job. 

Explicit or automatic roll back of a job reverts the locked and modified objects back to their original value based on the job's journal. Any new objects added using the job will be deleted upon roll back, and any objects deleted using the job will be put back upon roll back. Rolling back a job also removes the job, the job's journal, and all locks associated with the job. 

All o.io operations executed within a job are automatically raised for status, and if an operation fails for any reason then the job is automatically rolled back and either RollbackCausedByException or FailedToRollBack is raised depending on whether the rollback was successful or failed. The RollbackCausedByException and FailedToRollBack custom exception classes include exception_causing_rollback and stacktrace_causing_rollback fields which contain the original exception and associated stacktrace that caused the automatic roll back. If the roll back method is called explicitly by the consumer and the roll back fails then those two fields will be empty. The FailedToRollBack custom exception class also includes exception_failing_rollback and stacktrace_failing_rollback fields containing the exception and associated stacktrace that caused the roll back itself to fail. If a roll back fails then the curator is expected to roll back the job and clean up. 

## Curators

The sole purpose of a curator is to monitor the 'oiot-locks' and 'oiot-jobs' collections in o.io and curate any timed out transactions by rolling back the job's journal entries and deleting the job and its locks. Curator instances can be run across multiple machines and are designed to run in a one-active configuration where all curators compete to be the active curator and only one curator actively curates at any given time. The run_curator.py convenience script is available for running a curator instance as a service.

## Configuration

The following settings are available in \_\_init\_\_.py for configuring oiot behavior:

```python
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

# elapsed time before a curator is timed out and another should take its place
_curator_heartbeat_timeout_in_ms = 7500

# time an inactive curator should sleep between status checks
_curator_inactivity_delay_in_ms = 3000

# elapsed time before a job is timed out and automatically rolled back
_max_job_time_in_ms = 5000

# additional elapsed time used by active curators before rolling back jobs
_additional_timeout_wait_in_ms = 1000

# value used by journal items to indicate that a delete operation was performed
_deleted_object_value = {"deleted": "{A0981677-7933-4A5C-A141-9B40E60BD411}"}
```

## Usage

Oiot is compatible with both Python 2.7 and 3.4. Common oiot use cases are as follows:

```python
from oiot import Job, OiotClient

# create an oiot client
client = OiotClient(YOUR_API_KEY)

# OiotClient provides all of the same functionality porc.Client and
# can be used as such
item = client.get(COLLECTION, KEY)
item['was_modified'] = True
client.put(item.collection, item.key, item.json, item.ref).raise_for_status()

# when a transaction is required use Job
job = Job(self._client)
job.post(COLLECTION1, VALUE) # locks the newly added key
job.put(COLLECTION2, KEY, VALUE) # locks the specified key
job.delete(COLLECTION3, KEY) # locks the specified key
job.get(COLLECTION4, KEY) # locks the specified key
job.complete() # completes the job and removes the locks

# attempting to access a locked key using OiotClient raises CollectionKeyIsLocked
job.put(COLLECTION2, KEY, VALUE) # locks the specified key
client.put(COLLECTION2, KEY, VALUE) # raises CollectionKeyIsLocked

# to ignore an existing lock set raise_if_locked to False when using 
# the OiotClient instance:
job.put(COLLECTION2, KEY, VALUE) # locks the specified key
client.put(COLLECTION2, KEY, VALUE, raise_if_locked=False) # ignores the lock

# to handle all exceptions raised by a job
try:
    job = Job(self._client)
    job.post(COLLECTION1, VALUE)
    job.put(COLLECTION2, KEY, VALUE)
    job.complete()
except RollbackCausedByException, FailedToRollBack, FailedToComplete: 
    ...

# to explicitly roll back a job use job.roll_back()
job = Job(self._client)
job.post(COLLECTION1, VALUE)
job.put(COLLECTION2, KEY, VALUE)
job.delete(COLLECTION3, KEY) # locks the specified key
# roll back removes the record from COLLECTION1, reverts the record in 
# COLLECTION2, puts back the record in COLLECTION3, and removes the 
# locks and the job
job.roll_back()
