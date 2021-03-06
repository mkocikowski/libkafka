Package libkafka is a low level golang library for producing to and consuming
from Kafka 1.0+. It has no external dependencies. It is not modeled on the Java
client. All API calls are synchronous and all code executes in the calling
goroutine.


Project Scope
---
The library focuses on non transactional production and consumption. It
implements single partition Producer and Consumer. Multi partition producers
and consumers are built on top of this library (example: https://github.com/mkocikowski/kafkaclient).


Development status / "roadmap"
---
As of 2020-04-30 focus in on producer code. Consumer code has been validated (I
have a working multi-partition consumer with sticky consumption coordinated
over group membership protocol built on top of kafkaclient library) but that
was just to make sure there were no design blind alleys. Next steps will be
partitioned production and mtls. Consumer work will come after that.

Get Started
---
Read the documentation for the "batch" and "client" packages. Everything is in
[godoc](https://pkg.go.dev/github.com/mkocikowski/libkafka).


Design Decisions
---
1. Focus on record batches. Kafka protocol Produce and Fetch API calls operate
on sets of record batches. Record batch is the unit at which messages are
produced and fetched. It also is the unit at which data is partitioned and
compressed. In libkafka producers and consumers operate on batches of records.
Building and parsing of record batches is separate from Producing and Fetching.
Record batch compression and decompression implementations are provided by the
library user.
2. Synchronous single-partition calls. Kafka wire protocol is asynchronous: on
a single connection there can be multiple requests awaiting response from the
Kafka broker. In addition, many API calls (such as Produce and Fetch) can
combine data for multiple topics and partitions in a single call. Libkafka
maintains a separate connection for every topic-partition and calls on that
connection are synchronous, and each call is for only one topic-partition. That
makes call handling (and failure) logic simpler.
3. Wide use of reflection. All API calls (requests and responses) are defined
as structs and marshaled using reflection. This is not a performance problem,
because API calls are not frequent. Marshaling and unmarshaling of individual
records within record batches (which has big performance impact) is done
without using reflection.
4. Limited use of data hiding. The library is not intended to be child proof.
Most internal structures are exposed to make debugging and metrics collection
easier.
