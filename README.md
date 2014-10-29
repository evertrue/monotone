monotone
========

A 64 bit monotonically increasing Id generator for distributed databases.

[![Build Status](https://travis-ci.org/evertrue/monotone.svg)](https://travis-ci.org/evertrue/monotone)

# Purpose
As soon as you make a departure from an RDBMS for record based storage, you find yourself needing to generate Id's for those records as it's no longer afforded to you in most No-Sql stores.

We wanted to provide a simple solution to this that reused popular infrastructure that companies likely already have running. 

# Design

* Monotone is a very simple wrapper around popular storage enginges capable of generating monotonically increasing id's that are not vulnerable to the challenges surrounding coorindated system clocks.

* Underyling storage engine maven depdencies are scoped to `provided` so as to not pollute your depdendency tree for unused storage engine implementations.

* A clean interface with fluent style builders and no checked exceptions is exposed to your codebase.

* Monotone stores local ranges after it reserves them by incrementing a counter in the underlying storage engine implemtnation via atomic supported operations. This permits fast increments via a local `AtomicLong` in your application and only making a network hop when you have exhausted the local range.


# ZooKeeper Support

### Details 
You may want to tune the `setMaxIdsToFetch` coniguration to your needs as this will dictate how frequent you have to do a write to ZK. 

This implementation has an upper bound to its performance, even with a higher limit on `setMaxIdsToFetch` configured. This is mainly because the implementation does not use sharded counters. This is under consideration for future development.

If `setMaxAttempts` is reached, a `RuntimeException` is thrown. 

### Dead Simple Example
```java
IdGenerator gen = ZKGenerator.newBuilder(zkClient).build();             
long nextId = gen.nextId();
```

### Configuration Example
```java
IdGenerator gen = ZKGenerator.newBuilder(zkClient)
                             .setCounterName("orders")
                             .setMaxIdsToFetch(1000)
                             .setRootPath("/id_gen/my_app")
                             .build();
                             
long nextId = gen.nextId();
```

### Configuration
|Setting|Default Value|Description|
|-------|:-------------:|-----------|
|`setCounterName`|"default"|The name of your counter that will be stored in ZK. Cannot be null.|
|`setMaxIdsToFetch`|1000|The number of Id's to reserve for a given local range. Needs to be greater than 0.|
|`setRootPath`|"/monotone/id_gen"|The root path in ZK where the counter should live. Cannot be null.|
|`setMaxAttempts`|5|The number of times an `incr` operation will occur if a failure response is given by ZK. Needs to be greater than 0.|

# Redis Support

# License
[Free as in beer](https://github.com/evertrue/monotone/blob/master/LICENSE)

