# Experimenting with MongoDB index creation

This is a small Spring Boot application created to experiment with creating MongoDB indices.

## Prerequisites

* JDK >= 17
* MongoDB or Docker

This is a Spring Boot 3 application, which requires Java 17 or newer.
If you don't have MongoDB installed, you can start an instance with Docker using the following command:

```
docker run --name mongo-indices -d -p27017:27017 mongo
```

## Experimenting

If you look in `application.yml`, you'll see that `spring.data.mongodb.auto-index-creation` is set to `true`.
As a result, Spring will automatically create indices based on annotations, such as `@Indexed` and `@CompoundIndex`.
This is one way of creating indices with Spring.

You can also create indices in a more manual way, using `MongoClient`, `MongoTemplate`, or `ReactiveMongoTemplate`.

The test `ExperimentApplicationTest` contains a number of tests that let you play around with the various ways of creating indices.

When you run `savesEntity` with an empty database, you'll see that creating indices based on annotations takes almost no time.
However, after running `savesEntities` one or more times, this is no longer the case.
After deleting the indices that Spring created and running `savesEntities` again, you'll see that it takes a few seconds to create the indices.
Both `@Indexed` and `@CompoundIndex` make it possible to specify that index creation should happen in the background,
but this has no effect in recent versions of MongoDB.

With MongoDB versions before 4.2, indices could be created in the foreground or the background.
Foreground builds would be faster and would lead to more efficient index data structures, but would block access to the database during the build.
Background builds would not block access to the database, but would take longer to build and be less efficient.

Starting from version 4.2, access is no longer blocked the entire time while the index is built.
Access is blocked at the start and end of the build process, however.

Even though access to the database is not blocked while indices are created,
the Spring application *will* block until the index creation is done.
Because of this, you should think twice about letting Spring handle index creation automatically.
Adding a new index for an existing collection could lead to long startup times if the collection is big.

The test `createsIndexViaTemnplate` uses `MongoTemplate` to create an index.
The behavior is similar to what happens when you let Spring create indices.
The method `ensureIndex` blocks until the index is created.
Contrary to what the name and documentation suggest, `ensureIndex` throws an exception if the index already exists.

The test `createsIndexReactively` uses `ReactiveMongoTemplate` to create an index.
In this case, the method `ensureIndex` doesn't block.

The test `createsIndexViaClient` uses `MongoClient` to create an index.
The method `createIndex` also doesn't block.
Contrary to `ReactiveMongoTemplate`, `MongoClient` doesn't require an additional dependency.

The tests `findsAllByBoolean`, `findsAllByInt`, `findsAllByIntGreaterThan`, and `findsAllByString` show how indices influence query performance.

