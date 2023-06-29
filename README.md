Warning: this extension is currently in an experimental state. Feel free to try it out, but be aware
that only minimal testing was done and no benchmarking.

# DuckDB Azure Extension
This extension adds a filesystem abstraction for Azure blob storage to DuckDB.

## Usage
Authentication is done by setting the connection string:

```SQL
SET azure_storage_connection_string = '<your_connection_string>';
```

After setting the connection string, azure blob storage can be queried:
```SQL
SELECT count(*) FROM 'azure://<my_container>/<my_file>.<parquet_or_csv>';
```

Blobs are also supported:
```SQL
SELECT * from 'azure://<my_container>/*.csv';
```

Check out the tests in `test/sql` for more examples.

## Building
This extension depends on the Azure c++ sdk. To build it, either install that manually, or use vcpkg
to do dependency management. To install vcpkg check out the docs [here](https://vcpkg.io/en/getting-started.html).
Then to build this extension run:

```shell
VCPKG_TOOLCHAIN_PATH=<path_to_your_vcpkg_toolchain> make
```