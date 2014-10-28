monotone
========

A 64 bit monotonically increasing Id generator for distributed databases.

# Purpose
As soon as you make a departure from an RDBMS for record based storage, you find yourself needing to generate Id's for those records as it's no longer afforded to you in most No-Sql stores.

We wanted to provide a simple solution to this that reused popular infrastructure that companies likely already have running. 

# Design

* Monotone is a very simply wrapper around popular storage enginges capable of generating monotonically increasing id's that are not vulnerable to the challenges surrounding coorindated system clocks.

* Underyling storage engine maven depdencies are scoped to `provided` so as to not pollute your depdendency tree for unused storage engine implementations.

* A clean interface with fluent style builders and no checked exceptions is exposed to your codebase.

* Monotone stores local ranges after it reserves them by incrementing a counter in the underlying storage engine implemtnation via CAS supported operations. This permits fast increments via an `AtomicLong` in your application and only making a network hop when you have exhausted the local range.


# ZooKeeper Support

### Details 
One caveat to be aware of is that this implementation uses compare and set (CAS) operations which opens up the possibility that it will not complete in a desired amount of time or iterations under heavy concurrent load. In the event this happens, an `RuntimeException` is thrown. A way to counteract this would be to increase the `setMaxIdsToFetch` size, resulting in less trips to ZooKeeper to reserve local ranges.

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
|`setCounterName`|"default"|The name of your counter that will be stored in ZK|
|`setMaxIdsToFetch`|1000|The number of Id's to reserve for a given local range|
|`setRootPath`|"/monotone/id_gen"|The root path in ZK where the counter should live|

# Redis Support

# License
[Free as in beer](https://github.com/evertrue/monotone/blob/master/LICENSE)

