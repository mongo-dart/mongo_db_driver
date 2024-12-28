@Timeout(Duration(minutes: 10))
library;

import 'dart:async';
import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:crypto/crypto.dart' as crypto;
import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import '../utils/matcher/mongo_dart_error_matcher.dart';

const dbName = 'test-mongo-dart';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

late MongoClient client;
late MongoDatabase db;
Uuid uuid = Uuid();
List<String> usedCollectionNames = [];

String getRandomCollectionName() {
  var name = 'c-${uuid.v4()}';
  usedCollectionNames.add(name);
  return name;
}

Future testDbCreate() async {
  var client = MongoClient(defaultUri);
  await client.connect();
  await client.close();

  client = MongoClient(defaultUri);
  await client.connect();
  await client.close();
}

Future testOperationNotInOpenState() async {
  var client = MongoClient(defaultUri);
  var dbCreate = client.db();
  var coll = dbCreate.collection('test-error');
  expect(() async => await coll.findOneAndUpdate({'value': 1}, {'value': 1}),
      throwsMongoDartError);

  client = MongoClient(defaultUri);
  await client.connect();
  dbCreate = client.db();
  coll = dbCreate.collection('test-error');
  await client.close();

  expect(() async => await coll.findOneAndUpdate({'value': 1}, {'value': 1}),
      throwsMongoDartError);
}

/* 
Future testDbConnectionString() async {
  var db = Db('mongodb://www.example.com');
  expect(db.uriList.first, 'mongodb://www.example.com');
  db = Db('mongodb://www.example.com:27317');
  expect(db.uriList.first, 'mongodb://www.example.com:27317');
  db = Db.pool([
    'mongodb://www.example.com:27017',
    'mongodb://www.example.com:27217',
    'mongodb://www.example.com:27317'
  ]);
  expect(db.uriList.first, 'mongodb://www.example.com:27017');
  expect(db.uriList[1], 'mongodb://www.example.com:27217');
  expect(db.uriList.last, 'mongodb://www.example.com:27317');
  db = Db.pool([
    'mongodb://www.example.com:27017/test',
    'mongodb://www.example.com:27217/test',
    'mongodb://www.example.com:27317/test'
  ]);
  expect(db.uriList[1], 'mongodb://www.example.com:27217/test');
  db = Db('mongodb://www.example.com:27017,www.example.com:27217,'
      'www.example.com:27317/test');
  expect(db.uriList.first, 'mongodb://www.example.com:27017/test');
  expect(db.uriList[1], 'mongodb://www.example.com:27217/test');
  expect(db.uriList.last, 'mongodb://www.example.com:27317/test');
  // As a syntactic sugar we accept also blnak after comma,
  //   even if it should not be correct.
  db = Db('mongodb://www.example.com:27017, www.example.com:27217, '
      'www.example.com:27317/test');
  expect(db.uriList.first, 'mongodb://www.example.com:27017/test');
  expect(db.uriList[1], 'mongodb://www.example.com:27217/test');
  expect(db.uriList.last, 'mongodb://www.example.com:27317/test');
}
 */
Future testGetCollectionInfos() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertMany([
    {'a': 1}
  ]);
  var collectionInfos = await db.getCollectionInfos({'name': collectionName});

  expect(collectionInfos, hasLength(1));

  await collection.drop();
}

Future testRemove() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertMany([
    {'a': 1}
  ]);

  var collectionInfos = await db.getCollectionInfos({'name': collectionName});
  expect(collectionInfos, hasLength(1));

  var coll = db.collection(collectionName);
  await coll.deleteMany();
  //await db.removeFromCollection(collectionName);

  var allCollectionDocuments = await collection.find().toList();
  expect(allCollectionDocuments, isEmpty);
}

Future testDropDatabase() async {
  if (db.server.serverCapabilities.fcv != null &&
      db.server.serverCapabilities.fcv!.compareTo('6.0') >= 0) {
    await db.dropDb();
    return;
  }
  await db.dropDatabase();
}

Future testRunCommand() async {
  if (db.server.serverCapabilities.fcv != null &&
      db.server.serverCapabilities.fcv!.compareTo('6.0') >= 0) {
    var ret = await db.runCommand({'ping': 1});
    expect(ret[keyOk], 1.0);

    var ret2 =
        await CommandOperation(db, {'ping': 1}, <String, Object>{}).process();
    expect(ret2[keyOk], 1.0);

    ret2 = await PingCommand(db.mongoClient).process();
    expect(ret[keyOk], 1.0);

    ret2 = await db.pingCommand();
    expect(ret[keyOk], 1.0);

    var result = await db.collection(r'$cmd').findOne(filter: {'ping': 1});
    expect(result?[keyOk], 1.0);
    return;
  }
}
/* 
Future testGetNonce() async {
  if (db.server.serverCapabilities.fcv != null &&
      db.server.serverCapabilities.fcv!.compareTo('6.0') >= 0) {
    return;
  }
  var result = await db.getNonce();
  expect(result['ok'], 1);
} */

Future getBuildInfo() async {
  // Todo to be checked
/*   var result = await db.getBuildInfo();
  expect(result['ok'], 1); */
}

Future testIsMaster() async {
  // Todo to be checked
  /* var result = await db.isMaster();
  expect(result['ok'], 1); */
}

Future<void> testServerStatus() async {
  Map<String, dynamic> dbStatus = await db.serverStatus();
  if (dbStatus.isNotEmpty && dbStatus['ok'] == 1.0) {
    expect(dbStatus['ok'], 1.0);
    expect(dbStatus['version'], db.server.serverStatus.version);
    expect(dbStatus['process'], db.server.serverStatus.process);
    expect(dbStatus['host'], db.server.serverStatus.host);
    var storageEngineMap = dbStatus['storageEngine'];
    if (storageEngineMap != null && storageEngineMap['name'] == 'wiredTiger') {
      expect(
          storageEngineMap['persistent'], db.server.serverStatus.isPersistent);
      if (dbStatus['version'].compareTo('4.0') > 0) {
        expect(dbStatus['wiredTiger']['log']['maximum log file size'] > 0,
            db.server.serverStatus.isJournaled);
      }
    }
  }
}

MongoCollection testCollectionCreation() {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);
  return collection;
}

Future testEachOnEmptyCollection() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var count = 0;
  var sum = 0;

  await for (var document in collection.find()) {
    sum += document['a'] as int;
    count++;
  }

  expect(sum, 0);
  expect(count, 0);
}

Future testFindEachWithThenClause() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var count = 0;
  var sum = 0;
  await collection.insertMany([
    {'name': 'Vadim', 'score': 4},
    {'name': 'Daniil', 'score': 4},
    {'name': 'Nick', 'score': 5}
  ]);

  await for (var document in collection.find()) {
    sum += document['score'] as int;
    count++;
  }

  expect(sum, 13);
  expect(count, 3);
}

Future testDateTime() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertMany([
    {'day': 1, 'posted_on': DateTime.utc(2013, 1, 1)},
    {'day': 2, 'posted_on': DateTime.utc(2013, 1, 2)},
    {'day': 3, 'posted_on': DateTime.utc(2013, 1, 3)},
    {'day': 4, 'posted_on': DateTime.utc(2013, 1, 4)},
    {'day': 5, 'posted_on': DateTime.utc(2013, 1, 5)},
    {'day': 6, 'posted_on': DateTime.utc(2013, 1, 6)},
    {'day': 7, 'posted_on': DateTime.utc(2013, 1, 7)},
    {'day': 8, 'posted_on': DateTime.utc(2013, 1, 8)},
    {'day': 9, 'posted_on': DateTime.utc(2013, 1, 9)}
  ]);

  var result = await collection
      .find(filter: where..$lt('posted_on', DateTime.utc(2013, 1, 5)))
      .toList();

  expect(result.length, 4);
}

void testFindEach() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var count = 0;
  var sum = 0;
  await collection.insertMany([
    {'name': 'Vadim', 'score': 4},
    {'name': 'Daniil', 'score': 4},
    {'name': 'Nick', 'score': 5}
  ]);

  await for (var document in collection.find()) {
    count++;
    sum += document['score'] as int;
  }

  expect(count, 3);
  expect(sum, 13);
}

Future testFindStream() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var count = 0;
  var sum = 0;
  await collection.insertMany([
    {'name': 'Vadim', 'score': 4},
    {'name': 'Daniil', 'score': 4},
    {'name': 'Nick', 'score': 5}
  ]);

  await for (var document in collection.find()) {
    count++;
    sum += document['score'] as int;
  }

  expect(count, 3);
  expect(sum, 13);
}

Future testDrop() async {
  var collectionName = getRandomCollectionName();

  await db.drop(collectionName);
}

Future testSaveWithIntegerId() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[
    {'_id': 1, 'name': 'a', 'value': 10},
    {'_id': 2, 'name': 'b', 'value': 20},
    {'_id': 3, 'name': 'c', 'value': 30},
    {'_id': 4, 'name': 'd', 'value': 40}
  ];

  await collection.insertMany(toInsert);
  var result = await collection.findOne(filter: {'name': 'c'});
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['value'], 30);

  result = await collection.findOne(filter: {'_id': 3}) ?? <String, dynamic>{};
  expect(result, isNotNull);

  result['value'] = 2;
  await collection.replaceOne({'_id': 3}, result);

  result = await collection.findOne(filter: {'_id': 3}) ?? <String, dynamic>{};
  expect(result, isNotNull);

  expect(result['value'], 2);

  result = await collection.findOne(filter: where..$eq('_id', 3)) ??
      <String, dynamic>{};
  expect(result, isNotNull);

  expect(result['value'], 2);

  final notThere = {'_id': 5, 'name': 'd', 'value': 50};
  await collection.insertOne(notThere);
  result = await collection.findOne(filter: where..$eq('_id', 5)) ??
      <String, dynamic>{};
  expect(result, isNotNull);

  expect(result['value'], 50);
}

Future testSaveWithObjectId() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[
    {'name': 'a', 'value': 10},
    {'name': 'b', 'value': 20},
    {'name': 'c', 'value': 30},
    {'name': 'd', 'value': 40}
  ];

  await collection.insertMany(toInsert);
  var result = await collection.findOne(filter: {'name': 'c'});
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['value'], 30);

  var id = result['_id'];
  result = await collection.findOne(filter: {'_id': id});
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['value'], 30);

  result['value'] = 1;
  await collection.replaceOne({'_id': result['_id']}, result);
  result = await collection.findOne(filter: {'_id': id});
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['value'], 1);
}

Future testInsertWithObjectId() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  dynamic id;
  var objectToSave = <String, dynamic>{
    '_id': ObjectId(),
    'name': 'a',
    'value': 10
  };
  id = objectToSave['_id'];
  await collection.insertOne(objectToSave);

  var result = await collection.findOne(filter: where..$eq('name', 'a'));
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['_id'], id);
  expect(result['value'], 10);
}

Future testCount() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await insertManyDocuments(collection, 167);

  //var result = await collection.legacyCount();
  var result = await collection.count();

  expect(result.count, 167);
}

Future testDistinct() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertOne({'foo': 1});
  await collection.insertOne({'foo': 2});
  await collection.insertOne({'foo': 2});
  await collection.insertOne({'foo': 3});
  await collection.insertOne({'foo': 3});
  await collection.insertOne({'foo': 3});
  var result = await collection.distinctMap('foo');

  final values = result['values'] as List;
  expect(values[0], 1);
  expect(values[1], 2);
  expect(values[2], 3);
}

Future testAggregate() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];

  // Avg 1 with 1 rating
  toInsert.add({
    'game': 'At the Gates of Loyang',
    'player': 'Dallas',
    'rating': 1,
    'v': 1
  });

  // Avg 3 with 1 rating
  toInsert.add({'game': 'Age of Steam', 'player': 'Paul', 'rating': 3, 'v': 1});

  // Avg 2 with 2 ratings
  toInsert.add({'game': 'Fresco', 'player': 'Erin', 'rating': 3, 'v': 1});
  toInsert.add({'game': 'Fresco', 'player': 'Dallas', 'rating': 1, 'v': 1});

  // Avg 3.5 with 4 ratings
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Paul', 'rating': 4, 'v': 1});
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Erin', 'rating': 5, 'v': 1});
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Dallas', 'rating': 4, 'v': 1});
  toInsert.add(
      {'game': 'Ticket To Ride', 'player': 'Anthony', 'rating': 2, 'v': 1});

  // Avg 4.5 with 4 ratings (counting only highest v)
  toInsert.add({'game': 'Dominion', 'player': 'Paul', 'rating': 5, 'v': 2});
  toInsert.add({'game': 'Dominion', 'player': 'Erin', 'rating': 4, 'v': 1});
  toInsert.add({'game': 'Dominion', 'player': 'Dallas', 'rating': 4, 'v': 1});
  toInsert.add({'game': 'Dominion', 'player': 'Anthony', 'rating': 5, 'v': 1});

  // Avg 5 with 2 ratings
  toInsert.add({'game': 'Pandemic', 'player': 'Erin', 'rating': 5, 'v': 1});
  toInsert.add({'game': 'Pandemic', 'player': 'Dallas', 'rating': 5, 'v': 1});

  await collection.insertMany(toInsert);

  // Avg player ratings
  // Dallas = 3, Anthony 3.5, Paul = 4, Erin = 4.25
/* We want equivalent of this when used on the mongo shell.
 * (Should be able to just copy and paste below once test is run and failed once)
db.runCommand(
{ aggregate : "testAggregate", pipeline : [
{"$group": {
      "_id": { "game": "$game", "player": "$player" },
      "rating": { "$sum": "$rating" } } },
{"$group": {
        "_id": "$_id.game",
        "avgRating": { "$avg": "$rating" } } },
{ "$sort": { "_id": 1 } }
]});
 */
  var pipeline = [];
  var p1 = {
    '\$group': {
      '_id': {'game': '\$game', 'player': '\$player'},
      'rating': {'\$sum': '\$rating'}
    }
  };
  var p2 = {
    '\$group': {
      '_id': '\$_id.game',
      'avgRating': {'\$avg': '\$rating'}
    }
  };
  var p3 = {
    '\$sort': {'_id': 1}
  };

  pipeline.add(p1);
  pipeline.add(p2);
  pipeline.add(p3);

  expect(p1['\u0024group'], isNotNull);
  expect(p1['\$group'], isNotNull);

  /*  var v = await collection.aggregate(pipeline);
  final result = v['result'] as List; */
  // Todo to be checked
  /* var v = await collection.aggregate(pipeline, cursor: {});
  var cursor = v['cursor'] as Map;
  var result = cursor['firstBatch'] as List;
  expect(result[0]['_id'], 'Age of Steam');
  expect(result[0]['avgRating'], 3); */
}

Future testAggregateWithCursor() async {
  if (!db.server.serverCapabilities.supportsOpMsg) {
    var collectionName = getRandomCollectionName();
    var collection = db.collection(collectionName);

    var toInsert = <Map<String, dynamic>>[];

    // Avg 1 with 1 rating
    toInsert.add({
      'game': 'At the Gates of Loyang',
      'player': 'Dallas',
      'rating': 1,
      'v': 1
    });

    // Avg 3 with 1 rating
    toInsert
        .add({'game': 'Age of Steam', 'player': 'Paul', 'rating': 3, 'v': 1});

    // Avg 2 with 2 ratings
    toInsert.add({'game': 'Fresco', 'player': 'Erin', 'rating': 3, 'v': 1});
    toInsert.add({'game': 'Fresco', 'player': 'Dallas', 'rating': 1, 'v': 1});

    // Avg 3.5 with 4 ratings
    toInsert
        .add({'game': 'Ticket To Ride', 'player': 'Paul', 'rating': 4, 'v': 1});
    toInsert
        .add({'game': 'Ticket To Ride', 'player': 'Erin', 'rating': 5, 'v': 1});
    toInsert.add(
        {'game': 'Ticket To Ride', 'player': 'Dallas', 'rating': 4, 'v': 1});
    toInsert.add(
        {'game': 'Ticket To Ride', 'player': 'Anthony', 'rating': 2, 'v': 1});

    // Avg 4.5 with 4 ratings (counting only highest v)
    toInsert.add({'game': 'Dominion', 'player': 'Paul', 'rating': 5, 'v': 2});
    toInsert.add({'game': 'Dominion', 'player': 'Erin', 'rating': 4, 'v': 1});
    toInsert.add({'game': 'Dominion', 'player': 'Dallas', 'rating': 4, 'v': 1});
    toInsert
        .add({'game': 'Dominion', 'player': 'Anthony', 'rating': 5, 'v': 1});

    // Avg 5 with 2 ratings
    toInsert.add({'game': 'Pandemic', 'player': 'Erin', 'rating': 5, 'v': 1});
    toInsert.add({'game': 'Pandemic', 'player': 'Dallas', 'rating': 5, 'v': 1});

    await collection.insertMany(toInsert);

    // Avg player ratings
    // Dallas = 3, Anthony 3.5, Paul = 4, Erin = 4.25
/* We want equivalent of this when used on the mongo shell.
 * (Should be able to just copy and paste below once test is run and failed once)
db.runCommand(
{ aggregate : "testAggregate", pipeline : [
{"$group": {
      "_id": { "game": "$game", "player": "$player" },
      "rating": { "$sum": "$rating" } } },
{"$group": {
        "_id": "$_id.game",
        "avgRating": { "$avg": "$rating" } } },
{ "$sort": { "_id": 1 } }
]});
 */
    var pipeline = [];
    var p1 = {
      '\$group': {
        '_id': {'game': '\$game', 'player': '\$player'},
        'rating': {'\$sum': '\$rating'}
      }
    };
    var p2 = {
      '\$group': {
        '_id': '\$_id.game',
        'avgRating': {'\$avg': '\$rating'}
      }
    };
    var p3 = {
      '\$sort': {'_id': 1}
    };

    pipeline.add(p1);
    pipeline.add(p2);
    pipeline.add(p3);

    expect(p1['\u0024group'], isNotNull);
    expect(p1['\$group'], isNotNull);

    // Todo to be checked
    /* var v = await collection.aggregate(pipeline, cursor: {'batchSize': 3});
    final cursor = v['cursor'] as Map;
    expect(cursor['id'], const TypeMatcher<int>());
    expect(
        cursor['firstBatch'], allOf(const TypeMatcher<List>(), hasLength(3)));
    final firstBatch = cursor['firstBatch'] as List;
    expect(firstBatch[0]['_id'], 'Age of Steam');
    expect(firstBatch[0]['avgRating'], 3); */
  }
}

/* 
Future testAggregateToStream() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var skipTest = false;
  var buildInfo = await db.getBuildInfo();
  var versionArray = buildInfo['versionArray'];
  var versionNum = (versionArray[0] as num) * 100 + (versionArray[1] as num);
  if (versionNum < 206) {
    // Skip test for MongoDb server older then version 2.6
    skipTest = true;
    print(
        'testAggregateToStream skipped as server is older then version 2.6: ${buildInfo["version"]}');
    if (skipTest) {
      return;
    }
  }

  var toInsert = <Map<String, dynamic>>[];

  // Avg 1 with 1 rating
  toInsert.add({
    'game': 'At the Gates of Loyang',
    'player': 'Dallas',
    'rating': 1,
    'v': 1
  });

  // Avg 3 with 1 rating
  toInsert.add({'game': 'Age of Steam', 'player': 'Paul', 'rating': 3, 'v': 1});

  // Avg 2 with 2 ratings
  toInsert.add({'game': 'Fresco', 'player': 'Erin', 'rating': 3, 'v': 1});
  toInsert.add({'game': 'Fresco', 'player': 'Dallas', 'rating': 1, 'v': 1});

  // Avg 3.5 with 4 ratings
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Paul', 'rating': 4, 'v': 1});
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Erin', 'rating': 5, 'v': 1});
  toInsert
      .add({'game': 'Ticket To Ride', 'player': 'Dallas', 'rating': 4, 'v': 1});
  toInsert.add(
      {'game': 'Ticket To Ride', 'player': 'Anthony', 'rating': 2, 'v': 1});

  // Avg 4.5 with 4 ratings (counting only highest v)
  toInsert.add({'game': 'Dominion', 'player': 'Paul', 'rating': 5, 'v': 2});
  toInsert.add({'game': 'Dominion', 'player': 'Erin', 'rating': 4, 'v': 1});
  toInsert.add({'game': 'Dominion', 'player': 'Dallas', 'rating': 4, 'v': 1});
  toInsert.add({'game': 'Dominion', 'player': 'Anthony', 'rating': 5, 'v': 1});

  // Avg 5 with 2 ratings
  toInsert.add({'game': 'Pandemic', 'player': 'Erin', 'rating': 5, 'v': 1});
  toInsert.add({'game': 'Pandemic', 'player': 'Dallas', 'rating': 5, 'v': 1});

  await collection.insertMany(toInsert);

  // Avg player ratings
  // Dallas = 3, Anthony 3.5, Paul = 4, Erin = 4.25
/* We want equivalent of this when used on the mongo shell.
 * (Should be able to just copy and paste below once test is run and failed once)
db.runCommand(
{ aggregate : "testAggregate", pipeline : [
{"$group": {
      "_id": { "game": "$game", "player": "$player" },
      "rating": { "$sum": "$rating" } } },
{"$group": {
        "_id": "$_id.game",
        "avgRating": { "$avg": "$rating" } } },
{ "$sort": { "_id": 1 } }
]});
 */
  var pipeline = <Map<String, Object>>[];
  var p1 = {
    '\$group': {
      '_id': {'game': '\$game', 'player': '\$player'},
      'rating': {'\$sum': '\$rating'}
    }
  };
  var p2 = {
    '\$group': {
      '_id': '\$_id.game',
      'avgRating': {'\$avg': '\$rating'}
    }
  };
  var p3 = {
    '\$sort': {'_id': 1}
  };

  pipeline.add(p1);
  pipeline.add(p2);
  pipeline.add(p3);

  expect(p1['\u0024group'], isNotNull);
  expect(p1['\$group'], isNotNull);
  // set batchSize parameter to split response to 2 chunks
  var aggregate = await collection
      .aggregateToStream(pipeline,
          cursorOptions: {'batchSize': 1}, allowDiskUse: true)
      .toList();

  expect(aggregate.isNotEmpty, isTrue);
  expect(aggregate[0]['_id'], 'Age of Steam');
  expect(aggregate[0]['avgRating'], 3);
}
 */
Future testSkip() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await insertManyDocuments(collection, 600);

  var result = await collection.findOne(
      filter: where
        ..sortBy('a')
        ..skip(300));
  expect(result, isNotNull);
  if (result == null) {
    return;
  }

  expect(result['a'], 300);
}

Future testUpdateWithUpsert() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var (result, _, _, _) =
      await collection.insertOne({'name': 'a', 'value': 10});
  expect(result.isSuccess, true);

  var results = await collection.find(filter: {'name': 'a'}).toList();
  expect(results.length, 1);
  expect(results.first['name'], 'a');
  expect(results.first['value'], 10);

  var objectUpdate = {
    r'$set': {'value': 20}
  };
  var (resultUpdate, _) =
      await collection.updateOne({'name': 'a'}, objectUpdate);
  expect(resultUpdate.isSuccess, true);
  expect(resultUpdate.nModified, 1);

  results = await collection.find(filter: {'name': 'a'}).toList();
  expect(results.length, 1);
  expect(results.first['value'], 20);
}

Future testUpdateWithMultiUpdate() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var (result, _, _, _) = await collection.insertMany([
    {'key': 'a', 'value': 'initial_value1'},
    {'key': 'a', 'value': 'initial_value2'},
    {'key': 'b', 'value': 'initial_value_b'}
  ]);
  expect(result.isSuccess, true);

  var results = await collection.find(filter: {'key': 'a'}).toList();
  expect(results.length, 2);
  expect(results.first['key'], 'a');
  expect(results.first['value'], 'initial_value1');

  var (resultUpd, _) = await collection.updateOne(where..$eq('key', 'a'),
      modify..$set('value', 'value_modified_for_only_one_with_default'));
  expect(resultUpd.isSuccess, true);
  expect(resultUpd.nModified, 1);

  results = await collection.find(
      filter: {'value': 'value_modified_for_only_one_with_default'}).toList();
  expect(results.length, 1);

  (resultUpd, _) = await collection.updateOne(
    where..$eq('key', 'a'),
    modify..$set('value', 'value_modified_for_only_one_with_multiupdate_false'),
  );
  expect(resultUpd.isSuccess, true);
  expect(resultUpd.nModified, 1);

  results = await collection.find(filter: {
    'value': 'value_modified_for_only_one_with_multiupdate_false'
  }).toList();
  expect(results.length, 1);

  (resultUpd, _) = await collection.updateMany(
      where..$eq('key', 'a'), modify..$set('value', 'new_value'));
  expect(resultUpd.isSuccess, true);
  expect(resultUpd.nModified, 2);

  results = await collection.find(filter: {'value': 'new_value'}).toList();
  expect(results.length, 2);

  results = await collection.find(filter: {'key': 'b'}).toList();
  expect(results.length, 1);
  expect(results.first['value'], 'initial_value_b');
}

Future testLimitWithSortByAndSkip() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var counter = 0;

  await insertManyDocuments(collection, 1000);

  //var orderByMap = (where..sortBy('a')).map['orderby'];
  var operation = FindOperation(
    collection,
    QueryUnion({}),
    skip: 300,
    sort: SortUnion({'a': 1}),
    limit: 10,
  );

  var modernCursor = Cursor(operation, db.server);
  counter = await modernCursor.stream.length;
  expect(counter, 10);
  //expect(modernCursor.state, State.closed);
  expect(modernCursor.cursorId, Int64.ZERO);
  return;
}

Future<InsertManyDocumentRec> insertManyDocuments(
    MongoCollection collection, int numberOfRecords) async {
  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < numberOfRecords; n++) {
    toInsert.add({'a': n});
  }

  return await collection.insertMany(toInsert);
}

Future testLimit() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var counter = 0;
  await insertManyDocuments(collection, 30000);

  var operation = FindOperation(
    collection,
    QueryUnion({}),
    limit: 10,
  );

  var modernCursor = Cursor(operation, db.server);
  await modernCursor.stream.forEach((e) => counter++);
  expect(counter, 10);
  //expect(modernCursor.state, State.closed);
  expect(modernCursor.cursorId, Int64.ZERO);
  return;
}

Future<Cursor> testCursorCreation() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var cursor =
      Cursor(FindOperation(collection, QueryUnion({'ping': 1})), db.server);

  expect(cursor, isNotNull);

  return cursor;
}

Future testCursorClosing() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await insertManyDocuments(collection, 10000);

  var modernCursor =
      Cursor(FindOperation(collection, QueryUnion({})), db.server);

  //expect(modernCursor.state, State.init);

  var cursorResult = await modernCursor.nextObject();
  //expect(modernCursor.state, State.open);
  expect(modernCursor.cursorId.isNegative, isFalse);
  expect(cursorResult, isNotNull);
  if (cursorResult == null) {
    return;
  }
  expect(cursorResult['a'], 0);
  expect(cursorResult, isNotNull);

  await modernCursor.close();
  //expect(modernCursor.state, State.closed);
  expect(modernCursor.cursorId, Int64.ZERO);

  var result = await collection.findOne();
  expect(result, isNotNull);
}

Future testPingRaw() async {
  var collection = db.collection(r'$cmd');
  var modernCursor = Cursor(
      FindOperation(collection, QueryUnion({'ping': 1}), limit: 1), db.server);
  await modernCursor.nextObject();
}

Future testNextObject() async {
  var collection = db.collection(r'$cmd');
  var modernCursor = Cursor(
      FindOperation(collection, QueryUnion({'ping': 1}), limit: 1), db.server);

  var result = await modernCursor.nextObject();

  expect(result, containsPair('ok', 1));
}

Future testNextObjectToEnd() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);
  await collection.insertMany([
    {'a': 1},
    {'a': 2},
    {'a': 3}
  ]);

  var modernCursor =
      Cursor(FindOperation(collection, QueryUnion({}), limit: 10), db.server);

  var result = await modernCursor.nextObject();
  expect(result, isNotNull);
  result = await modernCursor.nextObject();
  expect(result, isNotNull);
  result = await modernCursor.nextObject();
  expect(result, isNotNull);
  result = await modernCursor.nextObject();
  expect(result, isNull);
}

Future testCursorWithOpenServerCursor() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await insertManyDocuments(collection, 1000);
  var modernCursor = Cursor(
      FindOperation(collection, QueryUnion({}),
          limit: 10, findOptions: FindOptions(batchSize: 5)),
      db.server);

  await modernCursor.nextObject();
  await modernCursor.nextObject();
  //expect(modernCursor.state, State.open);
  expect(modernCursor.cursorId.isNegative, isFalse);
}

Future testCursorGetMore() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var count = 0;
  var modernCursor =
      Cursor(FindOperation(collection, QueryUnion({}), limit: 10), db.server);

  count = await modernCursor.stream.length;

  expect(count, 0);

  await insertManyDocuments(collection, 1000);

  modernCursor = Cursor(FindOperation(collection, QueryUnion({})), db.server);

  count = await modernCursor.stream.length;
  expect(count, 1000);
  expect(modernCursor.cursorId, Int64.ZERO);
  //expect(modernCursor.state, State.closed);
}

/* 
void testDbCommandCreation() {
  var collectionName = getRandomCollectionName();

  var dbCommand = DbCommand(db, collectionName, 0, 0, 1, {}, {});
  expect(dbCommand.collectionNameBson?.value, '$dbName.$collectionName');
}
 */
Future testPingDbCommand() async {
  var res = await PingCommand(db.mongoClient).process();
  expect(res[keyOk], 1.0);
  res = await db.pingCommand();
  expect(res[keyOk], 1.0);
  res = await db.runCommand({'ping': 1});
  expect(res[keyOk], 1.0);
}

Future testDropDbCommand() async {
  var res = await db.dropDb();
  expect(res[keyOk], 1.0);
}
/* 
Future testIsMasterDbCommand() async {
  var isMasterCommand = DbCommand.createIsMasterCommand(db);

  var result = await db.queryMessage(isMasterCommand);

  expect(result.documents?[0], containsPair('ok', 1));
} */

String _md5(String value) => crypto.md5.convert(value.codeUnits).toString();
void testAuthComponents() {
  expect(_md5(''), 'd41d8cd98f00b204e9800998ecf8427e');
  expect(_md5('md4'), 'c93d3bf7a7c4afe94b64e30c2ce39f4f');
  expect(_md5('md5'), '1bc29b36f623ba82aaf6724fd3b16718');
  var nonce = '94505e7196beb570';
  var userName = 'dart';
  var password = 'test';
  var testKey = 'aea09fb38775830306c5ff6de964ff04';
  var hashedPassword = _md5('$userName:mongo:$password');
  var key = _md5('$nonce$userName$hashedPassword');
  expect(key, testKey);
}

Future testAuthenticationWithUri() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertOne({'a': 1});
  await collection.insertOne({'a': 2});
  await collection.insertOne({'a': 3});

  var foundValue = await collection.findOne();
  expect(foundValue, isNotNull);
  if (foundValue == null) {
    return;
  }

  expect(foundValue['a'], isNotNull);
}

Future testGetIndexes() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await insertManyDocuments(collection, 100);
  if (db.server.serverCapabilities.supportsOpMsg) {
    var indexes = await collection.listIndexes().toList();

    expect(indexes.length, 1);
    return;
  }
  var indexes = await collection.getIndexes();

  expect(indexes.length, 1);
}

Future testListIndexes() async {
  if (db.server.serverCapabilities.supportsOpMsg) {
    var collectionName = getRandomCollectionName();
    var collection = db.collection(collectionName);

    await insertManyDocuments(collection, 100);
    var indexes = await collection.listIndexes().toList();

    expect(indexes.length, 1);
  }
}

Future testIndexCreation() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 6; n++) {
    toInsert.add({
      'a': n,
      'embedded': {'b': n, 'c': n * 10}
    });
  }
  await collection.insertMany(toInsert);

  if (db.server.serverCapabilities.supportsOpMsg) {
    var res = await collection.createIndex(key: 'a', unique: true);
    expect(res['ok'], 1.0);

    res = await collection
        .createIndex(keys: {'a': -1, 'embedded.c': 1}, sparse: true);
    expect(res['ok'], 1.0);

    res = await collection.createIndex(keys: {
      'a': -1
    }, partialFilterExpression: {
      'embedded.c': {r'$exists': true}
    });
    expect(res['ok'], 1.0);

    var indexes = await collection.listIndexes().toList();
    expect(indexes.length, 4);

    expect(indexes[1]['unique'], isTrue);
    expect(indexes[1]['sparse'], isFalse);
    expect(indexes[2].containsKey('unique'), isFalse);
    expect(indexes[2]['sparse'], isTrue);
    expect(indexes[2]['name'], 'a_-1_embedded.c_1');
    expect(indexes[2].containsKey('partialFilterExpression'), isFalse);
    expect(indexes[3].containsKey('partialFilterExpression'), isTrue);

    res =
        (await db.ensureIndex(collectionName, keys: {'a': -1, 'embedded.c': 1}))
            as Map<String, dynamic>;
    expect(res['ok'], 1.0);
    return;
  }

  var res = await db.createIndex(collectionName, key: 'a');
  expect(res['ok'], 1.0);

  res = await db.createIndex(collectionName, keys: {'a': -1, 'embedded.c': 1});
  expect(res['ok'], 1.0);

  res = await db.createIndex(collectionName, keys: {
    'a': -1
  }, partialFilterExpression: {
    'embedded.c': {r'$exists': true}
  });
  expect(res['ok'], 1.0);

  var indexes = await collection.getIndexes();
  expect(indexes.length, 4);

  res = (await db.ensureIndex(collectionName, keys: {'a': -1, 'embedded.c': 1}))
      as Map<String, dynamic>;
  expect(res['ok'], 1.0);
}

Future testIndexCreationOnCollection() async {
  if (db.server.serverCapabilities.supportsOpMsg) {
    var collectionName = getRandomCollectionName();
    var collection = db.collection(collectionName);

    var toInsert = <Map<String, dynamic>>[];
    for (var n = 0; n < 6; n++) {
      toInsert.add({
        'a': n,
        'embedded': {'b': n, 'c': n * 10}
      });
    }
    await collection.insertMany(toInsert);

    /* var resInsert = */ await collection.insertOne({'a': 200},
        insertOneOptions:
            InsertOneOptions(writeConcern: WriteConcern.unacknowledged));

    // Todo correct
    //expect(resInsert['ok'], 1.0);

    var res = await collection.createIndex(key: 'a', unique: true);
    expect(res['ok'], 1.0);

    res = await collection
        .createIndex(keys: {'a': -1, 'embedded.c': 1}, sparse: true);
    expect(res['ok'], 1.0);

    res = await collection.createIndex(keys: {
      'a': -1
    }, partialFilterExpression: {
      'embedded.c': {r'$exists': true}
    });
    expect(res['ok'], 1.0);

    var indexes = await collection.getIndexes();
    expect(indexes.length, 4);

    expect(indexes[1]['unique'], isTrue);
    expect(indexes[1]['sparse'], isFalse);
    expect(indexes[2].containsKey('unique'), isFalse);
    expect(indexes[2]['sparse'], isTrue);
    expect(indexes[2]['name'], 'a_-1_embedded.c_1');
    expect(indexes[2].containsKey('partialFilterExpression'), isFalse);
    expect(indexes[3].containsKey('partialFilterExpression'), isTrue);
  }
}

Future testEnsureIndexWithIndexCreation() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 6; n++) {
    toInsert.add({
      'a': n,
      'embedded': {'b': n, 'c': n * 10}
    });
  }

  await collection.insertMany(toInsert);

  var result =
      await db.ensureIndex(collectionName, keys: {'a': -1, 'embedded.c': 1});
  expect(result['ok'], 1.0);
  expect(result['err'], isNull);
}

Future testIndexCreationErrorHandling() async {
  var collectionName = getRandomCollectionName();
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 6; n++) {
    toInsert.add({'a': n});
  }
  // Insert duplicate
  toInsert.add({'a': 3});

  await collection.insertMany(toInsert);

  try {
    await db.ensureIndex(collectionName, key: 'a', unique: true);
    fail("Expecting an error, but wasn't thrown");
  } on TestFailure {
    rethrow;
  } on Map catch (e) {
    expect(e[keyErrmsg] ?? e['err'],
        predicate((String msg) => msg.contains('duplicate key error')));
  }
}

Future testTextIndex() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 6; n++) {
    toInsert.add({
      'a': n,
      'embedded': {'b': n, 'c': n * 10}
    });
  }
  await collection.insertMany([
    {'_id': 1, 'name': 'Java Hut', 'description': 'Coffee and cakes'},
    {'_id': 2, 'name': 'Burger Buns', 'description': 'Gourmet hamburgers'},
    {'_id': 3, 'name': 'Coffee Shop', 'description': 'Just coffee'},
    {
      '_id': 4,
      'name': 'Clothes Clothes Clothes',
      'description': 'Discount clothing'
    },
    {'_id': 5, 'name': 'Java Shopping', 'description': 'Indonesian goods'}
  ]);

  var res = await collection
      .createIndex(keys: {'name': 'text', 'description': 'text'});
  expect(res['ok'], 1.0);

  var result = await collection.find(filter: {
    r'$text': {r'$search': 'java coffee shop'}
  }).toList();
  expect(result.length, 3);
  expect(result.every((element) {
    return (element['_id'] as num).remainder(2) == 1;
  }), isTrue);
}

Future testTtlIndex() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  // Here the CreateIndexOptions is set to null, but you can pass other
  // parameters if needed
  var indexOperation = CreateIndexOperation(db, collection, 'closingDate', null,
      rawOptions: {'expireAfterSeconds': 1});
  var res = await indexOperation.process();
  expect(res['ok'], 1.0);

  await collection.insertMany([
    {
      '_id': 1,
      'name': 'Java Hut',
      'description': 'Coffee and cakes',
      'closingDate': DateTime(2000)
    },
    {'_id': 2, 'name': 'Burger Buns', 'description': 'Gourmet hamburgers'},
    {'_id': 3, 'name': 'Coffee Shop', 'description': 'Just coffee'},
    {
      '_id': 4,
      'name': 'Clothes Clothes Clothes',
      'description': 'Discount clothing'
    },
    {'_id': 5, 'name': 'Java Shopping', 'description': 'Indonesian goods'}
  ]);

  // The cleanup on the server runs every 60 seconds, plus the time to build
  // the index, plus a little extra...
  await Future.delayed(Duration(seconds: 90));

  var elements = await collection.find().toList();
  expect(elements.length, 4);
}

Future testDropIndexCreationOnCollection() async {
  if (db.server.serverCapabilities.supportsOpMsg) {
    var collectionName = getRandomCollectionName();
    var collection = db.collection(collectionName);

    var res = await collection.createIndex(key: 'a', unique: true);
    expect(res['ok'], 1.0);

    res = await collection
        .createIndex(keys: {'a': -1, 'embedded.c': 1}, sparse: true);
    expect(res['ok'], 1.0);

    res = await collection.createIndex(keys: {
      'a': -1
    }, partialFilterExpression: {
      'embedded.c': {r'$exists': true}
    });
    expect(res['ok'], 1.0);

    var indexes = await collection.listIndexes().toList();
    expect(indexes.length, 4);

    await collection.dropIndexes(indexes[2][keyName]);

    indexes = await collection.listIndexes().toList();
    expect(indexes.length, 3);

    await collection.dropIndexes('*');

    indexes = await collection.listIndexes().toList();
    expect(indexes.length, 1);
  }
}

Future testSafeModeUpdate() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  for (var n = 0; n < 6; n++) {
    await collection.insertOne({
      'a': n,
      'embedded': {'b': n, 'c': n * 10}
    });
  }

  if (db.server.serverCapabilities.supportsOpMsg) {
    var (result, _) = await collection.updateOne({
      'a': 200
    }, {
      r'$set': {'a': 100}
    });
    expect(result.isSuccess, true);
    expect(result.nModified, 0);
    expect(result.nMatched, 0);

    (result, _) = await collection.updateOne({
      'a': 3
    }, {
      r'$set': {'a': 100}
    });
    expect(result.isSuccess, true);
    expect(result.nModified, 1);
    expect(result.nMatched, 1);
    return;
  }
  var (result, _) = await collection.updateOne({'a': 200}, {'a': 100});
  expect(result.nModified, 0);
  expect(result.nMatched, 0);

  (result, _) = await collection.updateOne({'a': 3}, {'a': 100});
  expect(result.nModified, 1);
  expect(result.nMatched, 1);
}

Future testFindWithFieldsClause() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertMany([
    {'name': 'Vadim', 'score': 4},
    {'name': 'Daniil', 'score': 4},
    {'name': 'Nick', 'score': 5}
  ]);

  var result = await collection.findOne(
      filter: where
        ..$eq('name', 'Vadim')
        ..selectFields(['score']));
  expect(result, isNotNull);
  if (result == null) {
    return;
  }
  expect(result['name'], isNull);
  expect(result['score'], 4);
}

Future testFindAndModify() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);
  dynamic result;

  await collection.insertMany([
    {'name': 'Bob', 'score': 2},
    {'name': 'Vadim', 'score': 4},
    {'name': 'Daniil', 'score': 4},
    {'name': 'Nick', 'score': 5},
    {'name': 'Alice', 'score': 1},
  ]);

  (_, result) = await collection.findOneAndUpdate(
      where..$eq('name', 'Vadim'), modify..$inc('score', 10),
      fields: (ProjectionExpression()
            ..includeField('score')
            ..excludeField('_id'))
          .build());
  expect(result['value']['_id'], isNull);
  expect(result['value']['name'], isNull);
  expect(result['value']['score'], 4);

  (_, result) = await collection.findOneAndUpdate(
      where..$eq('name', 'Daniil'), modify..$inc('score', 3),
      returnNew: true);
  expect(result['value']['_id'], isNotNull);
  expect(result['value']['name'], 'Daniil');
  expect(result['value']['score'], 7);

  (_, result) = await collection.findOneAndDelete(where..$eq('name', 'Nick'));
  expect(result['value']['_id'], isNotNull);
  expect(result['value']['name'], 'Nick');
  expect(result['value']['score'], 5);

  (_, result) = await collection.findOneAndUpdate(
      where..$eq('name', 'Unknown'), modify..$inc('score', 3));
  expect(result['value'], isNull);

  (_, result) = await collection.findOneAndUpdate(
      where..$eq('name', 'Unknown'), modify..$inc('score', 3),
      returnNew: true);
  expect(result['value'], isNull);

  (_, result) = await collection.findOneAndUpdate(
      where..sortBy('score'), modify..$inc('score', 100),
      returnNew: true);
  expect(result['value']['name'], 'Alice');
  expect(result['value']['score'], 101);

  (_, result) = await collection.findOneAndUpdate(
      where..$eq('name', 'New Comer'), modify..$inc('score', 100),
      returnNew: true, upsert: true);
  expect(result['value']['name'], 'New Comer');
  expect(result['value']['score'], 100);
}

Future testSimpleQuery() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  ObjectId id;
  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 10; n++) {
    toInsert.add({'my_field': n, 'str_field': 'str_$n'});
  }
  await collection.insertMany(toInsert);

  var result = await collection
      .find(
          filter: where
            ..$gt('my_field', 5)
            ..sortBy('my_field'))
      .toList();
  expect(result.length, 4);
  expect(result[0]['my_field'], 6);

  var result1 = await collection.findOne(filter: where..$eq('my_field', 3));
  expect(result1, isNotNull);
  if (result1 == null) {
    return;
  }
  expect(result1['my_field'], 3);
  id = result1['_id'] as ObjectId;
  expect(id.oid, id.oid);

  var result2 = await collection.findOne(filter: where..id(id));
  expect(result2, isNotNull);
  expect(result2?['my_field'], 3);

  await collection.deleteOne(where..id(id));
  var result3 = await collection.findOne(filter: where..$eq('my_field', 3));
  expect(result3, isNull);
}

Future testCompoundQuery() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  var toInsert = <Map<String, dynamic>>[];
  for (var n = 0; n < 10; n++) {
    toInsert.add({'my_field': n, 'str_field': 'str_$n'});
  }

  await collection.insertMany(toInsert);

  var result = await collection
      .find(
          filter: where
            ..$gt('my_field', 8)
            ..$or
            ..$lt('my_field', 2))
      .toList();
  expect(result.length, 3);

  var result1 = await collection.findOne(
      filter: where
        ..$gt('my_field', 8)
        ..$or
        ..$lt('my_field', 2)
        ..$and
        ..$eq('str_field', 'str_1'));
  expect(result1, isNotNull);
  expect(result1?['my_field'], 1);
}

Future testFieldLevelUpdateSimple() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  ObjectId id;
  if (db.server.serverCapabilities.supportsOpMsg) {
    var (resultInsert, _, _, _) =
        await collection.insertOne({'name': 'a', 'value': 10});
    expect(resultInsert.nInserted, 1);
  } else {
    var (_, resultInsert, _, _) =
        await collection.insertOne({'name': 'a', 'value': 10});
    expect(resultInsert['n'], 0);
  }

  var result = await collection.findOne(filter: {'name': 'a'});
  expect(result, isNotNull);

  id = result?['_id'] as ObjectId;
  if (db.server.serverCapabilities.supportsOpMsg) {
    var (writeResult, _) =
        await collection.updateOne(where..id(id), modify..$set('name', 'BBB'));
    expect(writeResult.isSuccess, true);
    expect(writeResult.nModified, 1);
  } else {
    var (res, _) =
        await collection.updateOne(where..id(id), modify..$set('name', 'BBB'));
    expect(res.nModified, 1);
    expect(res.nMatched, 1);
  }

  result = await collection.findOne(filter: where..id);
  expect(result, isNotNull);
  expect(result?['name'], 'BBB');
}

Future testQueryOnClosedConnection() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await client.close();
  expect(() async => collection.find().toList(), throwsMongoDartError);
}

Future testUpdateOnClosedConnection() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await client.close();
  expect(() async => await collection.insertOne({'test': 'test'}),
      throwsMongoDartError);
}

Future testReopeningDb() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await collection.insertOne({'one': 'test'});
  await client.close();
  await client.connect();
  collection = client.db().collection(collectionName);

  var result = await collection.findOne();

  expect(result, isNotNull);
}

Future testDbNotOpen() async {
  var collectionName = getRandomCollectionName();
  var collection = db.collection(collectionName);

  await client.close();
  expect(() async => collection.findOne(), throwsMongoDartError);
}

Future testDbOpenWhileStateIsOpening() {
  var collectionName = getRandomCollectionName();

  var client = MongoClient(defaultUri);
  return Future.sync(() {
    client.connect().then((_) {
      return client.db().collection(collectionName).findOne();
    }).then((res) {
      expect(res, isNull);
      client.close();
    });
    client.connect().then((_) {
      return client.db().collection(collectionName).findOne();
    }).then((res) {
      expect(res, isNull);
    }).catchError((e) {
      expect(e is MongoDartError, isTrue);
      //expect(db.state == State.opening, isTrue);
    });
  });
}

void testInvalidIndexCreationErrorHandling() {
  var collectionName = getRandomCollectionName();

  expect(() async => db.createIndex(collectionName /*, key: 'a'*/),
      throwsA((e) => e is ArgumentError));
}

void testInvalidIndexCreationErrorHandling1() {
  var collectionName = getRandomCollectionName();

  expect(() async => db.createIndex(collectionName, key: 'a', keys: {'a': -1}),
      throwsA((e) => e is ArgumentError));
}

Future testFindOneWhileStateIsOpening() async {
  var collectionName = getRandomCollectionName();

  var client = MongoClient(defaultUri);
  return Future.sync(() async {
    // ignore: unawaited_futures
    client.connect().then((_) {
      return client.db().collection(collectionName).findOne();
    }).then((res) {
      expect(res, isNull);
      client.close();
    });

    try {
      await client.db().collection(collectionName).findOne();
    } catch (e) {
      expect(e is MongoDartError, isTrue);
      //expect(db.state == State.opening, isTrue);
    }
  });
}

void main() async {
  Future initializeDatabase() async {
    client = MongoClient(defaultUri);
    await client.connect();
    db = client.db();
  }

  Future cleanupDatabase() async {
    await client.close();
  }

  group('A', () {
    setUp(() async {
      await initializeDatabase();
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    group('Db creation tests:', () {
      //test('test connection string', testDbConnectionString);
      test('test db.create()', testDbCreate);
      test('Error - operation not in Open state', testOperationNotInOpenState);
    });
    group('DbCollection tests:', () {
      test('testAuthComponents', testAuthComponents);
    });
    group('DBCommand:', () {
      test('testAuthenticationWithUri', testAuthenticationWithUri);
      test('testDropDatabase', testDropDatabase);
      test('testRunCommand', testRunCommand);
      test('testGetCollectionInfos', testGetCollectionInfos);
      test('testRemove', testRemove);
      //test('testGetNonce', testGetNonce);
      test('getBuildInfo', getBuildInfo);
      test('testIsMaster', testIsMaster);
      test('testServerStatus', testServerStatus);
    });

    group('DbCollection tests:', () {
      test('testCollectionCreation', testCollectionCreation);
      test('testLimitWithSortByAndSkip', testLimitWithSortByAndSkip);
      test('testLimitWithSkip', testLimit);
      test('testFindEachWithThenClause', testFindEachWithThenClause);
      test('testSimpleQuery', testSimpleQuery);
      test('testCompoundQuery', testCompoundQuery);
      test('testCount', testCount);
      test('testDistinct', testDistinct);
      test('testFindEach', testFindEach);
      test('testEach', testEachOnEmptyCollection);
      test('testDrop', testDrop);
      test('testSaveWithIntegerId', testSaveWithIntegerId);
      test('testSaveWithObjectId', testSaveWithObjectId);
      test('testInsertWithObjectId', testInsertWithObjectId);
      test('testSkip', testSkip);
      test('testDateTime', testDateTime);
      test('testUpdateWithUpsert', testUpdateWithUpsert);
      test('testUpdateWithMultiUpdate', testUpdateWithMultiUpdate);
      test('testFindWithFieldsClause', testFindWithFieldsClause);
      test('testFindAndModify', testFindAndModify);
    });

    group('Cursor tests:', () {
      test('testCursorCreation', testCursorCreation);
      test('testCursorClosing', testCursorClosing);
      test('testNextObjectToEnd', testNextObjectToEnd);
      test('testPingRaw', testPingRaw);
      test('testNextObject', testNextObject);
      test('testCursorWithOpenServerCursor', testCursorWithOpenServerCursor);
      test('testCursorGetMore', testCursorGetMore);
      test('testFindStream', testFindStream);
    });

    group('DBCommand tests:', () {
      //test('testDbCommandCreation', testDbCommandCreation);
      test('testPingDbCommand', testPingDbCommand);
      test('testDropDbCommand', testDropDbCommand);
      //test('testIsMasterDbCommand', testIsMasterDbCommand);
    });

    group('Safe mode tests:', () {
      test('testSafeModeUpdate', testSafeModeUpdate);
    });

    group('Indexes tests:', () {
      test('testGetIndexes', testGetIndexes);
      test('testListIndexes', testListIndexes);

      test('testIndexCreation', testIndexCreation);
      test('testIndexCreationOnCollection', testIndexCreationOnCollection);
      test(
          'testEnsureIndexWithIndexCreation', testEnsureIndexWithIndexCreation);
      test('testIndexCreationErrorHandling', testIndexCreationErrorHandling);
      test('Text index', testTextIndex);
      test('Ttl index', testTtlIndex);

      test('testDropIndexCreationOnCollection',
          testDropIndexCreationOnCollection);
    });

    group('Field level update tests:', () {
      test('testFieldLevelUpdateSimple', testFieldLevelUpdateSimple);
    });

    group('Aggregate:', () {
      test('testAggregate', testAggregate,
          skip: 'As of MongoDB 3.6, cursor is *required* for aggregate.');
      test('testAggregateWithCursor', testAggregateWithCursor);
      /*   test(
          'testAggregateToStream - if server older then version 2.6 test would be skipped',
          testAggregateToStream); */
    });
  });

  group('Error handling without opening connection before', () {
    test('testDbOpenWhileStateIsOpening', testDbOpenWhileStateIsOpening);
    test('testFindOneWhileStateIsOpening', testFindOneWhileStateIsOpening);
  });

  group('Error handling:', () {
    setUp(() async {
      await initializeDatabase();
    });

    tearDown(() async {
      try {
        await client.connect();
        db = client.db();
      } catch (e) {
        // db possibly already open
      }

      try {
        await client.close();
      } catch (e) {
        // db possibly already closed
      }
    });

    test('testQueryOnClosedConnection', testQueryOnClosedConnection);
    test('testUpdateOnClosedConnection', testUpdateOnClosedConnection);
    test('testReopeningDb', testReopeningDb);
    test('testDbNotOpen', testDbNotOpen);
    test('testInvalidIndexCreationErrorHandling',
        testInvalidIndexCreationErrorHandling /*,
        skip:
            'It seems to be perfectly valid code. '
                'No source for expected exception. '
                'TODO remeber how this test was created in the first plave'*/
        );
    test('testInvalidIndexCreationErrorHandling1',
        testInvalidIndexCreationErrorHandling1);
  });

  tearDownAll(() async {
    await client.connect();
    db = client.db();
    await Future.forEach(usedCollectionNames,
        (String collectionName) => db.collection(collectionName).drop());
    await client.close();
  });
}
