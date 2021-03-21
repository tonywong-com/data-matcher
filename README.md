# Data Matcher
Script to match and group user data in CSV, by
* email
* phone
* email OR phone

## Usage

Please follow these instructions to run the script.
```
# Install gems
bundle install

# Run data matcher
bundle exec ruby matcher.rb --path <path> --match_types <match_types>

# Examples. Please note, match_types is a comma seperated list with no space in between.
bundle exec ruby matcher.rb --path './data/input1.csv' --match_types email
bundle exec ruby matcher.rb --path './data/input2.csv' --match_types phone
bundle exec ruby matcher.rb --path './data/input3.csv' --match_types email,phone

# Usage information
bundle exec ruby matcher.rb --help
```

* Both input and output data files can be found in the `./data` folder

# Algorithm

This script is designed to use an average O(n) linear time algorithm.  It aims to scan the data set only once, and takes advantage of the average O(1) time complexity of hash table for read/write and record matching.

# Vertical Scalability

Vertical scalability can be achieved, bounded by memory limitation.

* The loading of CSV data is memory efficient because it scans the input data line-by-line and doesn't load the input data set into memory.
* However, this algorithm relies on the lookup hash-table being stored in memory.  We can only scale up this solution until all available memory is used up.
* For example, a memory_profiler has been used to run against the CSV outer loop in the `matcher_service`'s `run()` method, while using the `input3.csv` file with 20,001 records.   And here is the result:

```
Total allocated: 58750659 bytes (827498 objects)
Total retained:  2048909 bytes (39964 objects)
```

* The retained memory is ~2MB.  So a node with 4GB free memory should be able to handle up to 40 million records, without memory paging.
* In terms of processing time, here is the benchmark on a Intel Quad-Core i5 2.4GHz laptop:

```
Total run time: 00:00:00.449
```

* Based on this timing, a 40 million line CSV is estimated to take 15 minutes to process.

# Horizontal Scalability

With this algorithm, it is challenging to achieve horizontal scalability by running a shared memory store (e.g. Redis) and a cluster of compute nodes.

When processing each row, a compute node needs to lock the whole hash_table for both read and write, to avoid this race condition:
* NodeA look up phone from shared memory, thinking it is unique.
* While NodeA continues processing the same row, NodeB write this phone to the shared memory.  However, NodeB's record is actually a duplicate of NodeA's.
* Without table-locking, NodeA misses the chance to detect this deplicate, resulting in incorrect result.

Locking the whole shared memory most of the time defeats the purpose of horizontal scaling.

If we are to keep this algorithm, another way to scale horizontally is to shard the input data set based on one of its fields, or add constraint to the matching rules to make sharding possible.  Here are some examples:

* Shard the user records by regions, and only find duplicates within a region.
* Shard the user records by last_name.  This essentially introduce a new matching rule stating that a matching user must share the same last name.  Sharding the data by last_name's leading characters, allows almost inifinite numbers of data shards for horizontal scalability.

