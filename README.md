
# Go Lockstep Server

I want to be able to setup a lockstep stream off of any postgres table/view
that has the few fields required by the protocol, and it will stream that table
via lockstep, including any arbitrary data fields it includes.

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
