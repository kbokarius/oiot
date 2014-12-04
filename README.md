# oiot

A tool to facilitate transactions for Orchestrate.io (o.io). Consists of a Job class allowing for multiple o.io operations to be executed as an atomic transaction, an OiotClient class inherited from porc.Client that raises exceptions if an attempt is made to access a locked collection key, and a Curator class that's designed to run as a service and cleans up broken transactions. Since OiotClient not only provides the same methods as porc.Client but also maintains the same contracts, integrating oiot into an existing application is as easy as changing "client = porc.Client(API_KEY)" to "client = oiot.OiotClient(API_KEY)" and using the Job class whenever transactions are required. Jobs utilize journaling and locking mechanisms where both mechanisms execute under the covers to ease consumption and use. If an operation executed by a job fails for any reason then the job is automatically rolled back and either RollbackCausedByException or FailedToRollBack is raised depending on whether the rollback was successful or failed. If a roll back fails then the curator is expected to roll back the job and clean up. The sole purpose of a curator is to monitor the 'oiot-locks' and 'oiot-jobs' collections in o.io and curate any timed out transactions by rolling back the job's journal entries and deleting the job and its lock. Curator instances can be run across multiple machines and are designed to run in a one-active configuration where all curators compete to be the active curator and only one curator actively curates at any given time.

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
    job.roll_back() # removes the record in COLLECTION1,  reverts the record in COLLECTION2, and removes the locks
