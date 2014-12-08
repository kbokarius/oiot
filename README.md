## Overview 

A tool to enable and facilitate transactions for Orchestrate.io (o.io). Consists of a Job class allowing for multiple o.io operations to be executed as an atomic transaction, an OiotClient class inherited from porc.Client that raises exceptions if an attempt is made to access a locked collection key, and a Curator class that's designed to run as a service and clean up broken transactions. 

## OiotClient

The OiotClient class inherits from porc.Client and overrides the methods that need to check for existing locks for the specified collection keys prior to executing the corresponding operations. The methods currently overridden are put(), get(), and delete(). Note that "raise_if_locked=False" can be passed to these methods to ignore existing locks and revert to the standard porc.Client behavior. Since OiotClient not only provides the same methods as porc.Client but also maintains the same contracts, integrating oiot into an existing application is as easy as changing "client = porc.Client(API_KEY)" to "client = oiot.OiotClient(API_KEY)" and using the Job class whenever transactions are required.

## Jobs

Jobs utilize journaling and locking mechanisms where both mechanisms execute under the covers to ease consumption and use. All o.io operations executed within a job are automatically raised for status, and if an operation fails for any reason then the job is automatically rolled back and either RollbackCausedByException or FailedToRollBack is raised depending on whether the rollback was successful or failed.

The RollbackCausedByException and FailedToRollBack custom exception classes include exception_causing_rollback and stacktrace_causing_rollback fields which contain the original exception and associated stacktrace that caused the automatic roll back. If the roll back method is called explicitly by the consumer and the roll back fails then those two fields will be empty. The FailedToRollBack custom exception class also includes exception_failing_rollback and stacktrace_failing_rollback fields containing the exception and associated stacktrace that caused the roll back itself to fail. If a roll back fails then the curator is expected to roll back the job and clean up. 

Once all operations are executed via a job instance then the complete() method should be called to indicate that the job is complete. Completing a job removes the job, the job's journal, and all locks associated with the job. If a job fails to complete for any reason then a FailedToComplete custom exception is thrown including exception_failing_completion and stacktrace_failing_completion fields that contain the exception and stacktrace that caused the job completion to fail.

## Curators

The sole purpose of a curator is to monitor the 'oiot-locks' and 'oiot-jobs' collections in o.io and curate any timed out transactions by rolling back the job's journal entries and deleting the job and its locks. Curator instances can be run across multiple machines and are designed to run in a one-active configuration where all curators compete to be the active curator and only one curator actively curates at any given time. The run_curator.py convenience script is available for running a curator instance as a service.

## Configuration

The following settings are available in \_\_init\_\_.py for customizing oiot behavior:

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
```

## Usage

```python
from oiot import Job, OiotClient

# create an oiot client
client = OiotClient(YOUR_API_KEY)

# OiotClient provides all of the same functionality porc.Client and can be used as such
item = client.get(COLLECTION, KEY)
item['was_modified'] = True
client.put(item.collection, item.key, item.json, item.ref).raise_for_status()

# when a transaction is required use Job
job = Job(self._client)
job.post(COLLECTION1, VALUE).raise_for_status() # locks the newly added key
job.put(COLLECTION2, KEY, VALUE).raise_for_status() # locks the specified key
job.complete() # completes the job and removes the locks

# attempting to access a locked key using OiotClient raises CollectionKeyIsLocked
job.put(COLLECTION2, KEY, VALUE).raise_for_status() # locks the specified key
client.put(COLLECTION2, KEY, VALUE ) # raises CollectionKeyIsLocked

# to ignore an existing lock set raise_if_locked to False when using the OiotClient instance:
job.put(COLLECTION2, KEY, VALUE).raise_for_status() # locks the specified key
client.put(COLLECTION2, KEY, VALUE, raise_if_locked=False).raise_for_status() # ignores the lock

# to handle all exceptions raised by a job
try:
    job = Job(self._client)
    job.post(COLLECTION1, VALUE).raise_for_status()
    job.put(COLLECTION2, KEY, VALUE).raise_for_status()
    job.complete()
except RollbackCausedByException, FailedToRollBack: 
    ...

# to explicitly roll back a job use job.roll_back()
job = Job(self._client)
job.post(COLLECTION1, VALUE).raise_for_status()
job.put(COLLECTION2, KEY, VALUE).raise_for_status()
job.roll_back() # removes the record in COLLECTION1, reverts the record in COLLECTION2, and removes the locks
