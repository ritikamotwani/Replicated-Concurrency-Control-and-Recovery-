# Replicated Concurrency Control and Recovery

Implemented a distributed database, complete with serializable snapshot isolation, replication, and failure recovery. It gives a deep insight into the design of a distributed system.

## Overview
This project involves implementing a distributed database with the following features:
- Serializable Snapshot Isolation (SSI)
- Replication using the Available Copies approach
- Failure recovery

## Algorithms Used
- Available Copies approach to replication with SSI and validation at commit time
- Aborting writes on failed sites


## Concurrency Handling
- Detect concurrency-induced abortion conditions due to first committer wins and consecutive RW edges in a cycle
- No need to restart aborted transactions; handled by the application
