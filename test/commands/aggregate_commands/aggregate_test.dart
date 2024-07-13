@Timeout(Duration(minutes: 10))
library;

import 'package:bson/bson.dart';
import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/unions/hint_union.dart';
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
      test('RunCommand - Use \$currentOp on an Admin Database', () async {
        var result = await client.adminCommand({
          'aggregate': 1,
          'pipeline': [
            {
              r'$currentOp': {'allUsers': true, 'idleConnections': true}
            },
            {
              r'$match': {'shard': "shard01"}
            }
          ],
          'cursor': {}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });
      test('RunCommand - Return Information on the Aggregation Operation',
          () async {
        var result = await db.runCommand({
          'aggregate': collection!.collectionName,
          'pipeline': [
            {
              r'$match': {'status': "A"}
            },
            {
              r'$group': {
                '_id': r'$cust_id',
                'total': {r'$sum': r'$amount'}
              }
            },
            {
              r'$sort': {'total': -1}
            }
          ],
          'explain': true
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result['stages'];

        expect(ret, isNotNull);
        expect(ret.length, 2);
      });
      test('RunCommand - Aggregate Data Specifying Batch Size', () async {
        var result = await db.runCommand({
          'aggregate': collection!.collectionName,
          'pipeline': [
            {
              r'$match': {'status': 'A'}
            },
            {
              r'$group': {
                '_id': r'$cust_id',
                'total': {r'$sum': r'$amount'}
              }
            },
            {
              r'$sort': {'total': -1}
            },
            {r'$limit': 2}
          ],
          'cursor': {'batchSize': 0}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });
      test('RunCommand - Specify a Collation', () async {
        var result = await db.runCommand({
          'aggregate': collection2!.collectionName,
          'pipeline': [
            {
              r'$match': {'status': 'A'}
            },
            {
              r'$group': {
                '_id': r'$category',
                'count': {r'$sum': 1}
              }
            }
          ],
          'cursor': {},
          'collation': {'locale': 'fr', 'strength': 1}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first, containsPair('count', 3));
      });
      test('RunCommand - Hint an Index', () async {
        var result = await db.runCommand({
          'aggregate': collection3!.collectionName,
          'pipeline': [
            {
              r'$sort': {'qty': 1}
            },
            {
              r'$match': {'category': 'cake', 'qty': 10}
            },
            {
              r'$sort': {'type': -1}
            }
          ],
          'cursor': {},
          'hint': {'qty': 1, 'category': 1}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first,
            {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10});
      });
      test('RunCommand - Override Default Read Concern', () async {
        var result = await db.runCommand({
          'aggregate': collection3!.collectionName,
          'pipeline': [
            {
              r'$match': {
                'qty': {r'$lt': 18}
              }
            }
          ],
          'cursor': {},
          'readConcern': {'level': 'majority'}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 2);
        expect(ret.last,
            {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15});
      });
      test('RunCommand - Use Variables in let', () async {
        var result = await db.runCommand({
          'aggregate': collection4!.collectionName,
          'pipeline': [
            {
              r'$match': {
                r'$expr': {
                  r'$gt': [r'$salesTotal', r'$$targetTotal']
                }
              }
            },
          ],
          'cursor': {},
          'let': {'targetTotal': 3000}
        });

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(
            ret.last, {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350});
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

      test(' Use \$currentOp on an Admin Database', () async {
        var command = AggregateOperation([
          {
            r'$currentOp': {'allUsers': true, 'idleConnections': true}
          },
          {
            r'$match': {'shard': "shard01"}
          }
        ], db: db);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('Return Information on the Aggregation Operation', () async {
        var command = AggregateOperation([
          {
            r'$match': {'status': "A"}
          },
          {
            r'$group': {
              '_id': r'$cust_id',
              'total': {r'$sum': r'$amount'}
            }
          },
          {
            r'$sort': {'total': -1}
          }
        ], collection: collection, explain: true);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));
        expect(result.containsKey('explainVersion'), isTrue);

        var ret = result['stages'];

        expect(ret, isNotNull);
      });
      test('Aggregate Data Specifying Batch Size', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}))
          ..addStage($limit(2));
        var command = AggregateOperation(pipeline,
            collection: collection, cursor: {'batchSize': 0});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('Specify a Collation', () async {
        var command = AggregateOperation([
          {
            r'$match': {'status': 'A'}
          },
          {
            r'$group': {
              '_id': r'$category',
              'count': {r'$sum': 1}
            }
          }
        ],
            collection: collection2,
            aggregateOptions: AggregateOptions(
                collation: CollationOptions('fr', strength: 1)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first, containsPair('count', 3));
      });
      test('Hint an Index', () async {
        var command = AggregateOperation([
          {
            r'$sort': {'qty': 1}
          },
          {
            r'$match': {'category': 'cake', 'qty': 10}
          },
          {
            r'$sort': {'type': -1}
          }
        ], collection: collection3, hint: HintUnion({'qty': 1, 'category': 1}));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first,
            {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10});
      });
      test('Override Default Read Concern', () async {
        var command = AggregateOperation([
          {
            r'$match': {
              'qty': {r'$lt': 18}
            }
          }
        ],
            collection: collection3,
            aggregateOptions: AggregateOptions(
                readConcern: ReadConcern(ReadConcernLevel.majority)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 2);
        expect(ret.last,
            {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15});
      });
      test(' Use Variables in let', () async {
        var command = AggregateOperation(
            [
              {
                r'$match': {
                  r'$expr': {
                    r'$gt': [r'$salesTotal', r'$$targetTotal']
                  }
                }
              }
            ],
            collection: collection4,
            let: {'targetTotal': 3000});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(
            ret.last, {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350});
      });
    });
    group('Open - Pipeline Builder', () {
      MongoCollection? collection;
      MongoCollection? collection2;
      MongoCollection? collection3;
      MongoCollection? collection4;
      MongoCollection? collection5;

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
        var collectionName5 = getRandomCollectionName(usedCollectionNamesV1);
        collection5 = dbV1.collection(collectionName5);
        var (_, _, _, _) = await collection5!.insertMany([
          {'_id': 8751, 'title': 'The Banquet', 'author': 'Dante', 'copies': 2},
          {
            '_id': 8752,
            'title': 'Divine Comedy',
            'author': 'Dante',
            'copies': 1
          },
          {'_id': 8645, 'title': 'Eclogues', 'author': 'Dante', 'copies': 2},
          {
            '_id': 7000,
            'title': 'The Odyssey',
            'author': 'Homer',
            'copies': 10
          },
          {'_id': 7020, 'title': 'Iliad', 'author': 'Homer', 'copies': 10}
        ]);
      });

      test('pipelineBuilder - Aggregate Data with Multi-Stage Pipeline',
          () async {
        var pipeline = pipelineBuilder
          ..addStage($project(included: ['tags']))
          ..addStage($unwind(Field('tags')))
          ..addStage($group(id: r"$tags", fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipeline, collection: collection);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });

      test('pipelineBuilder -  Use \$currentOp on an Admin Database', () async {
        var pipeline = pipelineBuilder
          ..addStage($currentOp(allUsers: true, idleConnections: true))
          ..addStage($match(where..$eq('shard', 'shard01')));
        var command =
            AggregateOperation(pipeline, db: client.db(dbName: 'admin'));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('pipelineBuilder - Return Information on the Aggregation Operation',
          () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}));
        var command =
            AggregateOperation(pipeline, collection: collection, explain: true);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));
        expect(result.containsKey('explainVersion'), isTrue);

        print('------------\n$result\n------------');

        var ret = result['stages'];

        expect(ret, isNotNull);
      });
      test('pipelineBuilder - Aggregate Data Specifying Batch Size', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}))
          ..addStage($limit(2));
        var command = AggregateOperation(pipeline,
            collection: collection, cursor: {'batchSize': 0});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('pipelineBuilder - Specify a Collation', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage($group(id: r'$category', fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipeline,
            collection: collection2,
            aggregateOptions: AggregateOptions(
                collation: CollationOptions('fr', strength: 1)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first, containsPair('count', 3));
      });
      test('Hint an Index', () async {
        var pipeline = pipelineBuilder
          ..addStage($sort({'qty': 1}))
          ..addStage($match(where
            ..$eq('category', 'cake')
            ..$eq('qty', 10)))
          ..addStage($sort({'type': -1}));
        var command = AggregateOperation(pipeline,
            collection: collection3,
            hint: HintUnion({'qty': 1, 'category': 1}));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first,
            {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10});
      });
      test('Override Default Read Concern', () async {
        var pipeline = pipelineBuilder..addStage($match(where..$lt('qty', 18)));
        var command = AggregateOperation(pipeline,
            collection: collection3,
            aggregateOptions: AggregateOptions(
                readConcern: ReadConcern(ReadConcernLevel.majority)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 2);
        expect(ret.last,
            {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15});
      });
      test(' Use Variables in let', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(
              where..$expr($gt(Field('salesTotal'), Var('targetTotal')))));
        var command = AggregateOperation(pipeline,
            collection: collection4, let: {'targetTotal': 3000});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(
            ret.last, {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350});
      });
      test('No cursor returned', () async {
        var pipeline = pipelineBuilder
          ..addStage($group(
              id: Field('author'), fields: {'books': $push(Field('title'))}))
          ..addStage($out(coll: 'authors'));
        var command = AggregateOperation(pipeline, collection: collection5);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
        expect(result[keyCursor][keyId], Int64(0));
      });
    });
    group('V1', () {
      late MongoCollection collection;
      MongoCollection? collection2;
      MongoCollection? collection3;
      MongoCollection? collection4;

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

        var collectionName2 = getRandomCollectionName(usedCollectionNamesV1);
        collection2 = dbV1.collection(collectionName2);
        var (_, _, _, _) = await collection2!.insertMany([
          {'_id': 1, 'category': "café", 'status': "A"},
          {'_id': 2, 'category': "cafe", 'status': "a"},
          {'_id': 3, 'category': "cafE", 'status': "a"}
        ]);
        var collectionName3 = getRandomCollectionName(usedCollectionNamesV1);
        collection3 = dbV1.collection(collectionName3);
        var (_, _, _, _) = await collection3!.insertMany([
          {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10},
          {'_id': 2, 'category': 'cake', 'type': 'ice cream', 'qty': 25},
          {'_id': 3, 'category': 'pie', 'type': 'boston cream', 'qty': 20},
          {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15}
        ]);
        await collection3!.createIndex(keys: {'qty': 1, 'type': 1});
        await collection3!.createIndex(keys: {'qty': 1, 'category': 1});
        var collectionName4 = getRandomCollectionName(usedCollectionNamesV1);
        collection4 = dbV1.collection(collectionName4);
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
      test(' Use \$currentOp on an Admin Database', () async {
        var command = AggregateOperation([
          {
            r'$currentOp': {'allUsers': true, 'idleConnections': true}
          },
          {
            r'$match': {'shard': "shard01"}
          }
        ], db: dbV1);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('Return Information on the Aggregation Operation', () async {
        var command = AggregateOperation([
          {
            r'$match': {'status': "A"}
          },
          {
            r'$group': {
              '_id': r'$cust_id',
              'total': {r'$sum': r'$amount'}
            }
          },
          {
            r'$sort': {'total': -1}
          }
        ], collection: collection, explain: true);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));
        expect(result.containsKey('explainVersion'), isTrue);

        var ret = result['stages'];

        expect(ret, isNotNull);
      });
      test('Aggregate Data Specifying Batch Size', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}))
          ..addStage($limit(2));
        var command = AggregateOperation(pipeline,
            collection: collection, cursor: {'batchSize': 0});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('Specify a Collation', () async {
        var command = AggregateOperation([
          {
            r'$match': {'status': 'A'}
          },
          {
            r'$group': {
              '_id': r'$category',
              'count': {r'$sum': 1}
            }
          }
        ],
            collection: collection2,
            aggregateOptions: AggregateOptions(
                collation: CollationOptions('fr', strength: 1)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first, containsPair('count', 3));
      });
      test('Hint an Index', () async {
        var command = AggregateOperation([
          {
            r'$sort': {'qty': 1}
          },
          {
            r'$match': {'category': 'cake', 'qty': 10}
          },
          {
            r'$sort': {'type': -1}
          }
        ], collection: collection3, hint: HintUnion({'qty': 1, 'category': 1}));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first,
            {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10});
      });
      test('Override Default Read Concern', () async {
        var command = AggregateOperation([
          {
            r'$match': {
              'qty': {r'$lt': 18}
            }
          }
        ],
            collection: collection3,
            aggregateOptions: AggregateOptions(
                readConcern: ReadConcern(ReadConcernLevel.majority)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 2);
        expect(ret.last,
            {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15});
      });
      test(' Use Variables in let', () async {
        var command = AggregateOperation(
            [
              {
                r'$match': {
                  r'$expr': {
                    r'$gt': [r'$salesTotal', r'$$targetTotal']
                  }
                }
              }
            ],
            collection: collection4,
            let: {'targetTotal': 3000});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(
            ret.last, {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350});
      });
    });
    group('V1 - Piepline Builder', () {
      late MongoCollection collection;
      MongoCollection? collection2;
      MongoCollection? collection3;
      MongoCollection? collection4;
      MongoCollection? collection5;

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

        var collectionName2 = getRandomCollectionName(usedCollectionNamesV1);
        collection2 = dbV1.collection(collectionName2);
        var (_, _, _, _) = await collection2!.insertMany([
          {'_id': 1, 'category': "café", 'status': "A"},
          {'_id': 2, 'category': "cafe", 'status': "a"},
          {'_id': 3, 'category': "cafE", 'status': "a"}
        ]);
        var collectionName3 = getRandomCollectionName(usedCollectionNamesV1);
        collection3 = dbV1.collection(collectionName3);
        var (_, _, _, _) = await collection3!.insertMany([
          {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10},
          {'_id': 2, 'category': 'cake', 'type': 'ice cream', 'qty': 25},
          {'_id': 3, 'category': 'pie', 'type': 'boston cream', 'qty': 20},
          {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15}
        ]);
        await collection3!.createIndex(keys: {'qty': 1, 'type': 1});
        await collection3!.createIndex(keys: {'qty': 1, 'category': 1});
        var collectionName4 = getRandomCollectionName(usedCollectionNamesV1);
        collection4 = dbV1.collection(collectionName4);
        var (_, _, _, _) = await collection4!.insertMany([
          {'_id': 1, 'flavor': "chocolate", 'salesTotal': 1580},
          {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350},
          {'_id': 3, 'flavor': "cherry", 'salesTotal': 2150}
        ]);
        var collectionName5 = getRandomCollectionName(usedCollectionNamesV1);
        collection5 = dbV1.collection(collectionName5);
        var (_, _, _, _) = await collection5!.insertMany([
          {'_id': 8751, 'title': 'The Banquet', 'author': 'Dante', 'copies': 2},
          {
            '_id': 8752,
            'title': 'Divine Comedy',
            'author': 'Dante',
            'copies': 1
          },
          {'_id': 8645, 'title': 'Eclogues', 'author': 'Dante', 'copies': 2},
          {
            '_id': 7000,
            'title': 'The Odyssey',
            'author': 'Homer',
            'copies': 10
          },
          {'_id': 7020, 'title': 'Iliad', 'author': 'Homer', 'copies': 10}
        ]);
      });

      test('pipelineBuilder - Aggregate Data with Multi-Stage Pipeline',
          () async {
        var pipeline = pipelineBuilder
          ..addStage($project(included: ['tags']))
          ..addStage($unwind(Field('tags')))
          ..addStage($group(id: r"$tags", fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipeline, collection: collection);

        var result = await command.execute();

        print(result);

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 3);
      });
      test('pipelineBuilder -  Use \$currentOp on an Admin Database', () async {
        var pipeline = pipelineBuilder
          ..addStage($currentOp(allUsers: true, idleConnections: true))
          ..addStage($match(where..$eq('shard', 'shard01')));
        var command =
            AggregateOperation(pipeline, db: client.db(dbName: 'admin'));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });
      test('pipelineBuilder - Return Information on the Aggregation Operation',
          () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}));
        var command =
            AggregateOperation(pipeline, collection: collection, explain: true);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));
        expect(result.containsKey('explainVersion'), isTrue);

        print('------------\n$result\n------------');

        var ret = result['stages'];

        expect(ret, isNotNull);
      });
      test('pipelineBuilder - Aggregate Data Specifying Batch Size', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage(
              $group(id: r'$cust_id', fields: {'total': $sum(r'$amount')}))
          ..addStage($sort({'total': -1}))
          ..addStage($limit(2));
        var command = AggregateOperation(pipeline,
            collection: collection, cursor: {'batchSize': 0});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
      });

      test('pipelineBuilder - Specify a Collation', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(where..$eq('status', 'A')))
          ..addStage($group(id: r'$category', fields: {'count': $sum(1)}));
        var command = AggregateOperation(pipeline,
            collection: collection2,
            aggregateOptions: AggregateOptions(
                collation: CollationOptions('fr', strength: 1)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first, containsPair('count', 3));
      });
      test('Hint an Index', () async {
        var pipeline = pipelineBuilder
          ..addStage($sort({'qty': 1}))
          ..addStage($match(where
            ..$eq('category', 'cake')
            ..$eq('qty', 10)))
          ..addStage($sort({'type': -1}));
        var command = AggregateOperation(pipeline,
            collection: collection3,
            hint: HintUnion({'qty': 1, 'category': 1}));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(ret.first,
            {'_id': 1, 'category': "cake", 'type': 'chocolate', 'qty': 10});
      });
      test('Override Default Read Concern', () async {
        var pipeline = pipelineBuilder..addStage($match(where..$lt('qty', 18)));
        var command = AggregateOperation(pipeline,
            collection: collection3,
            aggregateOptions: AggregateOptions(
                readConcern: ReadConcern(ReadConcernLevel.majority)));
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 2);
        expect(ret.last,
            {'_id': 4, 'category': 'pie', 'type': 'blueberry', 'qty': 15});
      });
      test(' Use Variables in let', () async {
        var pipeline = pipelineBuilder
          ..addStage($match(
              where..$expr($gt(Field('salesTotal'), Var('targetTotal')))));
        var command = AggregateOperation(pipeline,
            collection: collection4, let: {'targetTotal': 3000});
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 1);
        expect(
            ret.last, {'_id': 2, 'flavor': "strawberry", 'salesTotal': 4350});
      });
      test('No cursor returned', () async {
        var pipeline = pipelineBuilder
          ..addStage($group(
              id: Field('author'), fields: {'books': $push(Field('title'))}))
          ..addStage($out(coll: 'authors'));
        var command = AggregateOperation(pipeline, collection: collection5);
        var result = await command.execute();

        expect(result, isNotNull);
        expect(result, containsPair(keyOk, 1.0));

        var ret = result[keyCursor][keyFirstBatch];

        expect(ret, isNotNull);
        expect(ret.length, 0);
        expect(result[keyCursor][keyId], Int64(0));
      });
    });
  });
}
