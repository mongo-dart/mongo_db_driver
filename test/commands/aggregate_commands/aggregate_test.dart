@Timeout(Duration(minutes: 10))
library;

import 'package:bson/bson.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart' hide MongoDocument;
import 'package:mongo_db_query/mongo_db_query.dart';
import 'dart:async';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test-mongo-dart-aggregate';
const dbNameV1 = 'test-mongo-dart-aggregate-v1';

const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';
const defaultUriV1 = 'mongodb://$dbAddress:27017/$dbNameV1';

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
  late MongoClient clientV1;
  late MongoDatabase dbV1;
  List<String> usedCollectionNamesV1 = [];

  group('Aggregate', () {
    setUpAll(() async {
      client = MongoClient(defaultUri);
      db = await initializeDatabase(client);
      clientV1 = MongoClient(defaultUriV1);
      dbV1 = await initializeDatabase(clientV1);
    });

    tearDownAll(() async {
      await Future.delayed(Duration(seconds: 1));

      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
      await Future.forEach(usedCollectionNamesV1,
          (String collectionName) => dbV1.collection(collectionName).drop());
      await clientV1.close();
    });

    // https://www.mongodb.com/docs/manual/reference/command/aggregate
    group('RunCommand', () {
      MongoCollection? collection;

      setUp(() async {
        var collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName!);
        var (_, MongoDocument result, _, _) = await collection!.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
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
      String? collectionName;

      setUp(() async {
        collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName!);
        var (_, MongoDocument result, _, _) = await collection!.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
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
      test('pipelineBuilder - Aggregate Data with Multi-Stage Pipeline',
          () async {
        var pipe = pipeline
          ..addStage($project(included: ['tags']))
          ..addStage($unwind(Field('tags')))
          ..addStage($group(id: r"$tags", fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipe, collection: collection);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });
    });
    group('V1', () {
      late MongoCollection collection;

      setUp(() async {
        var collectionName = getRandomCollectionName(usedCollectionNamesV1);
        collection = dbV1.collection(collectionName);
        var (result, _, _, _) = await collection.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
        ]);
        if (result.isFailure) {
          print(result.errmsg);
        }
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
      test('pipelineBuilder - Aggregate Data with Multi-Stage Pipeline',
          () async {
        var pipe = pipeline
          ..addStage($project(included: ['tags']))
          ..addStage($unwind(Field('tags')))
          ..addStage($group(id: r"$tags", fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipe, collection: collection);

        var result = await command.execute();

        print(result);

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });
    });
  });
}
