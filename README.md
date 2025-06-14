# filedb

A key-value store inspired by Bitcask.

--- 

FileDB is a Zig-implementation of Bitcask by Riak[^1] paper.

- FileDB stores record metadata in a log-structured hashtable and parallely keeps 1 disk file open for inserting records in append-only mode. On restarts or `MAX_FILE_REACHED`, the disk file is rotated and all the oldfiles are kept open for reading **only**. 
- A compaction process running every `config.compactionInterval` seconds, reads all the disk files and combines them into one file while updating the metadata hashtable.
- A sync process syncs the open disk files once every `config.syncInterval`. Sync also can be done on every request if `config.alwaysFsync` is True.

Read about internals in-depth at [FileDb](https://rajivharlalka.in/posts/filedb).

## Benefits:
1. Since the metadata keeps an exact location of file and position in file for a record, fetching records become O(1) operation.
2. All metadata records are constant size, so irrespective of the size of the value of a record the in-memory store keeps a constant sized metadata.
3. Provides high throughput by using the open file in append only mode.

## Methods
 
1.  `init(allocator: std.mem.Allocator, options: ?config.Options)` : Intialized FileDB
2. `deinit()`: Deinitalizes FileDB
3. `put(key:[]const u8, value: []const u8)`: Inserts a key-value pair in the database to be tracked.
4. `get(key:[]const u8)`: Retrieved a key-value pair from the database.
5. `delete(key: []const u8)`: Delete a key-value pair from the database
5. `list(allocator: std.mem.Allocator)`: Returns a list of keys stored in the database.
6. `sync()`: Syncs the current open datafile on the disk
7. `storeHashMap()`: Creates the HINTS file
8. `loadKeyDir()`: Loads the hashmap from the HINTS file

## Redis Compatible:

Along with the library, a Redis-compatible client is available.

```shell
127.0.0.1:6379> RING
(error) ERR unknown command
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> get abcd
(nil)
127.0.0.1:6379> set abcd def
OK
127.0.0.1:6379> get abcd
"def"
```

## Redis Benchmark

```shell
redis-benchmark -p 6379 -t set -n 10000 -r 100000000
Summary:
  throughput summary: 13736.26 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        3.615     0.088     3.455     6.831     8.831    14.919
      
redis-benchmark -p 6379 -t set -n 200000 -r 100000000
Summary:
  throughput summary: 14375.04 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        3.452     0.072     3.087     6.767    10.647   114.303

redis-benchmark -p 6379 -t get -n 100000 -r 100000000
Summary:
  throughput summary: 44286.98 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.573     0.088     0.519     0.967     1.447     7.495

redis-benchmark -p 6379 -t get -n 1000000 -r 1000000000 --threads 10
Summary:
  throughput summary: 104876.77 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.405     0.032     0.375     0.831     1.295    26.047
```

## References:

1. [Bitcask Paper by Riak](https://riak.com/assets/bitcask-intro.pdf)
2. [Go Implementation of Bitcask](https://github.com/mr-karan/barreldb)

## Zig Resources:
1. https://www.openmymind.net/Basic-MetaProgramming-in-Zig/
2. https://pedropark99.github.io/zig-book/
3. https://zig.guide/standard-library/
4. https://zighelp.org

[^1]: https://riak.com/assets/bitcask-intro.pdf