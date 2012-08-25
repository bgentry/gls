
# Go Lockstep Server

I want to be able to setup a lockstep stream off of any postgres table/view
that has the few fields required by the protocol, and it will stream that table
via lockstep, including any arbitrary data fields it includes.

## Output

Chunked HTTP

### JSON

#### Data rows

* A row will be returned as a JSON object:

    ```json
    {
     "txid": 549163,
     "current": 551033,
     "since": 549163,
     "deleted": true,
     "data": {
       "name": "mydomain.com",
       "user_id": 1
     }
    }
    ```

Fields:

* `txid`: transaction at which the record was last updated (required)
* `current`: `txid_snapshot_xmin(txid_snapshot_current())` (required)
* `since`: min of `txid_snapshot_xmin(txid_current_snapshot())` and `txid` (required)
* `deleted`: whether or not the row is marked as deleted
* `data`: any data fields in the row not part of the lockstep protocol

#### Control rows

* `snapshot_completed` (when the initial dump/snapshot is complete)

    ```json
    {
      "control": true,
      "type": "snapshot_completed",
      "timestamp": 123456789
    }
    ```

* `heartbeat` (after each set of updates is sent out or when there are no rows in last `n` seconds)

    ```json
    {
      "control": true,
      "type": "heartbeat",
      "timestamp": 123456789
    }
    ```

## Tests

Tests are good. To run them, you need to have a postgres server running locally
with a database named `gls_test`:

```bash
$ createdb gls_test
```

The test database URL is currently hardcoded as `postgres://localhost:5432/gls_test`
with no authentication and no SSL. Once you have that set up, you can run the tests:

```bash
$ go test
warning: building out-of-date packages:
	gls/logger
installing these packages with 'go test -i' will speed future tests.

PASS
ok  	gls	0.119s
```
