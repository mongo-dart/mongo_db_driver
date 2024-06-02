# Mongo_db_driver - MongoDB driver for Dart programming language

This package is a new version of a driver for connecting to a MongoDb server using Dart. It has been inspired from the mongo_dart package, but it has been reengineered to improve the user use and feel.

___________**THIS IS A PRE-ALPHA VERSION**

___________***NOT SUITABLE FOR PRODUCTION***

This package has been published only to allow early adopters tests.

APIs subject to change!

The library can be used in any environment with the exception of the Web one.

**NOTE**
Starting from this version of mongo_dart, we will on support releases starting from MongoDb 5.0.
All APIs hav been revisited to reflect the last versions of the commands.

## Differences with Mongo_dart

There are some differences with mongo_dart that must be noticed:

- The connection is done through a client object. Once that the connection is established, you can get a Database object. You can create different Database objects related to the same client instance.
- Sessions.
- Transactions.
- The connection pool has been redesigned.
- Advanced messaging.
- Automatic reconnection in case of network issues or elections.

## Apis

Apis normally are created to behave in the most similar way to the mongo shell.
Obviously not all and not necessarily in the same way, but at least the best possible way of performing a certain operation.
Naming convention also, as far as possible, is maintained equal to the one of the shell.

## Contribution

If you can contribute to the develpment of this package, your help is welcome!

## Basic usage

### Obtaining connection

```dart

  var client = MongoClient("mongodb://localhost:27017/mongo_dart-blog");
  await client.connect();
  var db = client.db();
```

### Opening a session

If you want to create a session, you could do the following:

```dart
var session = client.startSession();
  (ret, _, _, _) = await collection.insertOne(<String, dynamic>{
    '_id': 2,
    'name': 'Ezra',
    'state': 'active',
    'rating': 90,
    'score': 6
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }
  await session.endSession();
```

### Transactions

The following is a simple transaction example:

```dart
 session = client.startSession();
  session.startTransaction();
  (ret, _, _, _) = await collection.insertOne(<String, dynamic>{
    '_id': 3,
    'name': 'Nathan',
    'state': 'active',
    'rating': 98,
    'score': 4
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }
  var commitRes = await session.commitTransaction();
  if (commitRes?[keyOk] == 0.0) {
    print('${commitRes?[keyErrmsg]}');
  }
  await session.endSession();
```

### Check Examples

In the example folders there are many cases that you can check to learn how to use mongo_db_driver.
The documentation at present is not complete.
