@Timeout(Duration(minutes: 10))
library;

import 'package:bson/bson.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'dart:async';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test-mongo-dart-read-preference';

const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

Uuid uuid = Uuid();
List<String> usedCollectionNames = [];

String getRandomCollectionName(List<String> collectionNames) {
  var name = 'c-${uuid.v4()}';
  collectionNames.add(name);
  return name;
}

Future<MongoDatabase> initializeDatabase(MongoClient client) async {
  await client.connect();
  return client.db();
}

Future cleanupDatabase(MongoClient client) async => await client.close();

void main() async {
  late MongoClient client;
  late MongoDatabase db;
  List<String> usedCollectionNames = [];

  group('Aggregate', () {
    setUpAll(() async {
      client = MongoClient(defaultUri);
      db = await initializeDatabase(client);
    });

    tearDownAll(() async {
      await Future.delayed(Duration(seconds: 1));

      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
    });

    // https://www.mongodb.com/docs/manual/reference/command/aggregate
    group('Read Preference', () {
      MongoCollection? collection;
      MongoCollection? collection2;
      MongoCollection? collection3;
      MongoCollection? collection4;

      setUp(() async {
        var collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName);
        await collection!.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
        ]);
        var collectionName2 = getRandomCollectionName(usedCollectionNames);
        collection2 = db.collection(collectionName2);
        var (_, _, _, _) = await collection2!.insertMany([
          {'_id': 1, 'category': "café", 'status': "A"},
          {'_id': 2, 'category': "cafe", 'status': "a"},
          {'_id': 3, 'category': "cafE", 'status': "a"}
        ]);
        var collectionName3 = getRandomCollectionName(usedCollectionNames);
        collection3 = db.collection(collectionName3);
        var (_, _, _, _) = await collection3!.insertMany([
          {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10},
          {'_id': 2, 'category': 'cake', 'type': 'ice cream', 'qty': 25},
          {'_id': 3, 'category': 'pie', 'type': 'boston cream', 'qty': 20},
          {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15}
        ]);
        await collection3!.createIndex(keys: {'qty': 1, 'type': 1});
        await collection3!.createIndex(keys: {'qty': 1, 'category': 1});
        var collectionName4 = getRandomCollectionName(usedCollectionNames);
        collection4 = db.collection(collectionName4);
        var (_, _, _, _) = await collection4!.insertMany([
          {'_id': 1, 'flavor': "chocolate", 'salesTotal': 1580},
          {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350},
          {'_id': 3, 'flavor': "cherry", 'salesTotal': 2150}
        ]);
      });

      test('RunCommand - Aggregate Data with Multi-Stage Pipeline', () async {
        var result = await db.runCommand({
          'aggregate': collection!.collectionName,
          'pipeline': [
            {
              r'$project': {'tags': 1}
            },
            {r'$unwind': r'$tags'},
            {
              r'$group': {
                '_id': r"$tags",
                'count': {r'$sum': 1}
              }
            }
          ],
          'cursor': {}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });
    });
    group('Open', () {
      MongoCollection? collection;
      MongoCollection? collection2;
      MongoCollection? collection3;
      MongoCollection? collection4;
      setUp(() async {
        var collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName);
        await collection!.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
        ]);

        var collectionName2 = getRandomCollectionName(usedCollectionNames);
        collection2 = db.collection(collectionName2);
        var (_, _, _, _) = await collection2!.insertMany([
          {'_id': 1, 'category': "café", 'status': "A"},
          {'_id': 2, 'category': "cafe", 'status': "a"},
          {'_id': 3, 'category': "cafE", 'status': "a"}
        ]);
        var collectionName3 = getRandomCollectionName(usedCollectionNames);
        collection3 = db.collection(collectionName3);
        var (_, _, _, _) = await collection3!.insertMany([
          {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10},
          {'_id': 2, 'category': 'cake', 'type': 'ice cream', 'qty': 25},
          {'_id': 3, 'category': 'pie', 'type': 'boston cream', 'qty': 20},
          {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15}
        ]);
        await collection3!.createIndex(keys: {'qty': 1, 'type': 1});
        await collection3!.createIndex(keys: {'qty': 1, 'category': 1});
        var collectionName4 = getRandomCollectionName(usedCollectionNames);
        collection4 = db.collection(collectionName4);
        var (_, _, _, _) = await collection4!.insertMany([
          {'_id': 1, 'flavor': "chocolate", 'salesTotal': 1580},
          {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350},
          {'_id': 3, 'flavor': "cherry", 'salesTotal': 2150}
        ]);
      });

      test('Aggregate Data with Multi-Stage Pipeline', () async {
        var command = AggregateOperation([
          {
            r'$project': {'tags': 1}
          },
          {r'$unwind': r'$tags'},
          {
            r'$group': {
              '_id': r"$tags",
              'count': {r'$sum': 1}
            }
          }
        ], collection: collection);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });
    });
  });
}
