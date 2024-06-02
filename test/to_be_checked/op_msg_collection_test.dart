import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/mongo_dart_old.dart';
import 'package:mongo_db_driver/src/command/administration_commands/create_command/create_command.dart';
import 'package:mongo_db_driver/src/command/administration_commands/create_command/create_options.dart';
import 'package:decimal/decimal.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test-mongo-dart-collection';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

final Matcher throwsMongoDartError = throwsA(TypeMatcher<MongoDartError>());

late MongoClient client;
late MongoDatabase db;
Uuid uuid = Uuid();
List<String> usedCollectionNames = [];

String getRandomCollectionName() {
  var name = uuid.v4();
  usedCollectionNames.add(name);
  return name;
}

void main() async {
  Future initializeDatabase() async {
    client = MongoClient(defaultUri);
    await client.connect();
    db = client.db();
  }

  Future insertManyDocuments(
      MongoCollection collection, int numberOfRecords) async {
    var toInsert = <Map<String, dynamic>>[];
    for (var n = 0; n < numberOfRecords; n++) {
      toInsert.add({'a': n});
    }

    await collection.insertMany(toInsert);
  }

  Future cleanupDatabase() async {
    await client.close();
  }

  group('Collections', () {
    var cannotRunTests = false;
    setUp(() async {
      await initializeDatabase();
      if (!db.server.serverCapabilities.supportsOpMsg) {
        cannotRunTests = true;
      }
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    test('Simple create collection', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName).process();
      expect(resultMap[keyOk], 1.0);
      var collection = db.collection(collectionName);

      await insertManyDocuments(collection, 10000);
      var result = await collection.find().toList();
      expect(result.length, 10000);
    }, skip: cannotRunTests);

    test('Simple create capped collection', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName,
              createOptions:
                  CreateOptions(capped: true, size: 5242880, max: 5000))
          .process();
      expect(resultMap[keyOk], 1.0);
      var collection = db.collection(collectionName);

      await insertManyDocuments(collection, 10000);
      var result = await collection.find().toList();

      expect(result.length, 5000);
    });

    test('Simple create collection with schema', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName,
          createOptions: CreateOptions(validator: {
            r'$jsonSchema': {
              'bsonType': 'object',
              'required': ['phone'],
              'properties': {
                'phone': {
                  'bsonType': 'string',
                  'description': 'must be a string and is required'
                },
                'email': {
                  'bsonType': 'string',
                  'pattern': r'@mongodb\.com$',
                  'description':
                      'must be a string and match the regular expression pattern'
                },
                'status': {
                  'enum': ['Unknown', 'Incomplete'],
                  'description': 'can only be one of the enum values'
                }
              }
            }
          })).process();
      expect(resultMap[keyOk], 1.0);
      var collection = db.collection(collectionName);

      var (writeResult, _, _, _) = await collection.insertOne(
          {'name': 'Anand', 'phone': '451 3874643', 'status': 'Incomplete'},
          insertOneOptions:
              InsertOneOptions(writeConcern: WriteConcern.majority));
      expect(writeResult.isSuccess, isTrue);

      (writeResult, _, _, _) = await collection.insertOne(
          {'name': 'Amanda', 'status': 'Updated'},
          insertOneOptions:
              InsertOneOptions(writeConcern: WriteConcern.majority));
      expect(writeResult.isSuccess, isFalse);
      expect(writeResult.operationSucceeded, isTrue);
      expect(writeResult.hasWriteErrors, isTrue);
      expect(writeResult.writeError?.code, 121);
    });

    test('Simple create collection with no collation', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName).process();
      expect(resultMap[keyOk], 1.0);
      var collection = db.collection(collectionName);

      await collection.insertOne({'_id': 1, 'category': 'café'});
      await collection.insertOne({'_id': 2, 'category': 'cafe'});
      await collection.insertOne({'_id': 3, 'category': 'cafE'});

      var result = await collection
          .find(filter: QueryExpression()..sortBy('category'))
          .toList();

      expect(result, isNotNull);
      expect(result, isNotEmpty);
      expect(result.length, 3);
      expect(result.first['category'], 'cafE');
      expect(result.last['category'], 'café');
    });

    test('Simple create collection with collation', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName,
              createOptions: CreateOptions(collation: CollationOptions('fr')))
          .process();
      expect(resultMap[keyOk], 1.0);
      var collection = db.collection(collectionName);

      await collection.insertOne({'_id': 1, 'category': 'café'});
      await collection.insertOne({'_id': 2, 'category': 'cafe'});
      await collection.insertOne({'_id': 3, 'category': 'cafE'});

      var result =
          await collection.find(filter: where..sortBy('category')).toList();

      expect(result, isNotNull);
      expect(result, isNotEmpty);
      expect(result.length, 3);
      expect(result.first['category'], 'cafe');
      expect(result.last['category'], 'café');
    }, skip: cannotRunTests);

    test('Simple create collection with storage engine options', () async {
      var collectionName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, collectionName,
          createOptions: CreateOptions(storageEngine: {
            'wiredTiger': {
              'configString': 'log=(enabled),block_compressor=snappy'
            }
          })).process();
      expect(resultMap[keyOk], 1.0);
    });
  });

  group('Views', () {
    var cannotRunTests = false;
    setUp(() async {
      await initializeDatabase();
      if (!db.server.serverCapabilities.supportsOpMsg) {
        cannotRunTests = true;
      }
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    test('Simple create view', () async {
      var collectionName = 'abc';
      var viewName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(viewOn: collectionName, pipeline: [
            {
              r'$project': {
                'management': r'$feedback.management',
                'department': 1
              }
            }
          ])).process();
      expect(resultMap[keyOk], 1.0);

      var collection = db.collection(collectionName);
      var view = db.collection(viewName);
      await collection.insertOne({
        '_id': 1,
        'empNumber': 'abc123',
        'feedback': {'management': 3, 'environment': 3},
        'department': 'A'
      });
      await collection.insertOne({
        '_id': 2,
        'empNumber': 'xyz987',
        'feedback': {'management': 2, 'environment': 3},
        'department': 'B'
      });
      await collection.insertOne({
        '_id': 3,
        'empNumber': 'ijk555',
        'feedback': {'management': 3, 'environment': 4},
        'department': 'A'
      });

      var result = await view.find().toList();
      expect(result.first['department'], 'A');
      expect(result.first['management'], 3);
      expect(result[1]['department'], 'B');
      expect(result[1]['management'], 2);
    });

    test('Create view with aggregate sort', () async {
      var collectionName = 'abc';
      var viewName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(viewOn: collectionName, pipeline: [
            {
              r'$project': {
                'management': r'$feedback.management',
                'department': 1
              }
            },
            {r'$sortByCount': r'$department'}
          ])).process();
      expect(resultMap[keyOk], 1.0);

      var collection = db.collection(collectionName);
      var view = db.collection(viewName);
      await collection.insertOne({
        '_id': 1,
        'empNumber': 'abc123',
        'feedback': {'management': 3, 'environment': 3},
        'department': 'A'
      });
      await collection.insertOne({
        '_id': 2,
        'empNumber': 'xyz987',
        'feedback': {'management': 2, 'environment': 3},
        'department': 'B'
      });
      await collection.insertOne({
        '_id': 3,
        'empNumber': 'ijk555',
        'feedback': {'management': 3, 'environment': 4},
        'department': 'A'
      });

      var result = await view.find().toList();
      expect(result.first['_id'], 'A');
      expect(result.first['count'], 2);
      expect(result.last['_id'], 'B');
      expect(result.last['count'], 1);
    });
    test('Create view and aggregate later', () async {
      var collectionName = 'abc';
      var viewName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(viewOn: collectionName, pipeline: [
            {
              r'$project': {
                'management': r'$feedback.management',
                'department': 1
              }
            }
          ])).process();
      expect(resultMap[keyOk], 1.0);

      var collection = db.collection(collectionName);
      var view = db.collection(viewName);
      await collection.insertOne({
        '_id': 1,
        'empNumber': 'abc123',
        'feedback': {'management': 3, 'environment': 3},
        'department': 'A'
      });
      await collection.insertOne({
        '_id': 2,
        'empNumber': 'xyz987',
        'feedback': {'management': 2, 'environment': 3},
        'department': 'B'
      });
      await collection.insertOne({
        '_id': 3,
        'empNumber': 'ijk555',
        'feedback': {'management': 3, 'environment': 4},
        'department': 'A'
      });

      var result = await view.aggregateToStream([
        {r'$sortByCount': r'$department'}
      ]).toList();
      expect(result.first['_id'], 'A');
      expect(result.first['count'], 2);
      expect(result.last['_id'], 'B');
      expect(result.last['count'], 1);
    });

    test('Create a view from multiple collections', () async {
      var collection1Name = 'orders.a';
      var collection2Name = 'inventory.a';
      var collection1 = db.collection(collection1Name);
      var collection2 = db.collection(collection2Name);

      await collection1.insertOne({
        '_id': 1,
        'item': 'abc',
        'price': Decimal.parse('12.00'),
        'quantity': 2
      });
      await collection1.insertOne({
        '_id': 2,
        'item': 'jkl',
        'price': Decimal.parse('20.00'),
        'quantity': 1
      });
      await collection1.insertOne({
        '_id': 3,
        'item': 'abc',
        'price': Decimal.parse('10.95'),
        'quantity': 5
      });
      await collection1.insertOne({
        '_id': 4,
        'item': 'xyz',
        'price': Decimal.parse('5.95'),
        'quantity': 5
      });
      await collection1.insertOne({
        '_id': 5,
        'item': 'xyz',
        'price': Decimal.parse('5.95'),
        'quantity': 10
      });

      await collection2.insertOne(
          {'_id': 1, 'sku': 'abc', 'description': 'product 1', 'instock': 120});
      await collection2.insertOne(
          {'_id': 2, 'sku': 'def', 'description': 'product 2', 'instock': 80});
      await collection2.insertOne(
          {'_id': 3, 'sku': 'ijk', 'description': 'product 3', 'instock': 60});
      await collection2.insertOne(
          {'_id': 4, 'sku': 'jkl', 'description': 'product 4', 'instock': 70});
      await collection2.insertOne(
          {'_id': 5, 'sku': 'xyz', 'description': 'product 5', 'instock': 200});

      var viewName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(viewOn: collection1Name, pipeline: [
            {
              r'$lookup': {
                'from': collection2Name,
                'localField': 'item',
                'foreignField': 'sku',
                'as': 'inventory_docs'
              }
            },
            {
              r'$project': {'inventory_docs._id': 0, 'inventory_docs.sku': 0}
            }
          ])).process();
      expect(resultMap[keyOk], 1.0);

      var view = db.collection(viewName);

      var result = await view.find().toList();

      expect(result.length, 5);

      expect(result.first['item'], 'abc');
      expect(result.first['price'], Decimal.fromInt(12));
      expect(result.first['quantity'], 2);
      expect(result.first['inventory_docs'].first['instock'], 120);

      expect(result[1]['item'], 'jkl');
      expect(result[1]['price'], Decimal.parse('20.00'));
      expect(result[1]['quantity'], 1);
      expect(result[1]['inventory_docs'].first['instock'], 70);
    });

    test('Aggregation pipeline on a view from multiple collections', () async {
      var collection1Name = 'orders';
      var collection2Name = 'inventory';
      var collection1 = db.collection(collection1Name);
      var collection2 = db.collection(collection2Name);

      await collection1.insertOne({
        '_id': 1,
        'item': 'abc',
        'price': Decimal.parse('12.00'),
        'quantity': 2
      });
      await collection1.insertOne({
        '_id': 2,
        'item': 'jkl',
        'price': Decimal.parse('20.00'),
        'quantity': 1
      });
      await collection1.insertOne({
        '_id': 3,
        'item': 'abc',
        'price': Decimal.parse('10.95'),
        'quantity': 5
      });
      await collection1.insertOne({
        '_id': 4,
        'item': 'xyz',
        'price': Decimal.parse('5.95'),
        'quantity': 5
      });
      await collection1.insertOne({
        '_id': 5,
        'item': 'xyz',
        'price': Decimal.parse('5.95'),
        'quantity': 10
      });
      await collection1.insertOne({
        '_id': 6,
        'item': 'abc',
        'price': Decimal.parse('14.00'),
        'quantity': 4
      });

      await collection2.insertOne(
          {'_id': 1, 'sku': 'abc', 'description': 'product 1', 'instock': 120});
      await collection2.insertOne(
          {'_id': 2, 'sku': 'def', 'description': 'product 2', 'instock': 80});
      await collection2.insertOne(
          {'_id': 3, 'sku': 'ijk', 'description': 'product 3', 'instock': 60});
      await collection2.insertOne(
          {'_id': 4, 'sku': 'jkl', 'description': 'product 4', 'instock': 70});
      await collection2.insertOne(
          {'_id': 5, 'sku': 'xyz', 'description': 'product 5', 'instock': 200});

      var viewName = getRandomCollectionName();
      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(viewOn: collection1Name, pipeline: [
            {
              r'$lookup': {
                'from': 'inventory',
                'localField': 'item',
                'foreignField': 'sku',
                'as': 'inventory_docs'
              }
            },
            {
              r'$project': {'inventory_docs._id': 0, 'inventory_docs.sku': 0}
            }
          ])).process();
      expect(resultMap[keyOk], 1.0);

      var view = db.collection(viewName);

      var result = await view.aggregate([
        {r'$sortByCount': r'$item'}
      ]).toList();

      expect(result.length, 3);

      expect(result.first['_id'], 'abc');
      expect(result.first['count'], 3);
      expect(result.last['_id'], 'jkl');
      expect(result.last['count'], 1);
    });

    test('Create view with default Collation', () async {
      var collectionName = 'places';
      usedCollectionNames.add(collectionName);

      var collection = db.collection(collectionName);

      await collection.insertOne({'_id': 2, 'category': 'café'});
      await collection.insertOne({'_id': 3, 'category': 'cafe'});
      await collection.insertOne({'_id': 1, 'category': 'cafE'});
      await collection.insertOne({'_id': 4, 'category': 'lait'});
      await collection.insertOne({'_id': 5, 'category': 'Bière'});

      var viewName = getRandomCollectionName();
      usedCollectionNames.add(viewName);

      var resultMap = await CreateCommand(
        db,
        viewName,
        createOptions: CreateOptions(
            viewOn: collectionName,
            pipeline: [
              {
                r'$project': {'category': 1}
              }
            ],
            collation: CollationOptions('fr')),
      ).process();
      expect(resultMap[keyOk], 1.0);

      var view = db.collection(viewName);

      var result = await view
          .find(
            filter: QueryExpression()..sortBy('category'),
          )
          .toList();
      expect(result[1]['category'], 'cafe');
      expect(result[3]['category'], 'café');
    });

    test('Create view with default Collation - strength 1', () async {
      var collectionName = 'places2';
      usedCollectionNames.add(collectionName);

      var collection = db.collection(collectionName);

      await collection.insertOne({'_id': 2, 'category': 'café'});
      await collection.insertOne({'_id': 3, 'category': 'cafe'});
      await collection.insertOne({'_id': 1, 'category': 'cafE'});
      await collection.insertOne({'_id': 4, 'category': 'lait'});
      await collection.insertOne({'_id': 5, 'category': 'Bière'});

      var viewName = getRandomCollectionName();
      usedCollectionNames.add(viewName);

      var resultMap = await CreateCommand(db, viewName,
          createOptions: CreateOptions(
            viewOn: collectionName,
            pipeline: [
              {
                r'$project': {'category': 1}
              }
            ],
            /* collation: CollationOptions('fr', strength: 1) */
          ),
          rawOptions: {
            'collation': {'locale': 'fr', 'strength': 1}
          }).process();
      expect(resultMap[keyOk], 1.0);

      var view = db.collection(viewName);

      var countRet = await view.count(filter: {'category': 'cafe'});
      expect(countRet.count, 3);
    });

    test('Error overriding view default Collation', () async {
      var collectionName = 'places3';
      usedCollectionNames.add(collectionName);

      var collection = db.collection(collectionName);

      await collection.insertOne({'_id': 2, 'category': 'café'});
      await collection.insertOne({'_id': 3, 'category': 'cafe'});
      await collection.insertOne({'_id': 1, 'category': 'cafE'});
      await collection.insertOne({'_id': 4, 'category': 'lait'});
      await collection.insertOne({'_id': 5, 'category': 'Bière'});

      var viewName = getRandomCollectionName();
      usedCollectionNames.add(viewName);

      //getRandomCollectionName();
      var resultMap = await CreateCommand(
        db,
        viewName,
        createOptions: CreateOptions(
            viewOn: collectionName,
            pipeline: [
              {
                r'$project': {'category': 1}
              }
            ],
            collation: CollationOptions('fr')),
      ).process();
      expect(resultMap[keyOk], 1.0);

      var view = db.collection(viewName);

      try {
        await view
            .find(
                filter: where..sortBy('category'),
                findOptions:
                    FindOptions(collation: CollationOptions('fr', strength: 1)))
            .toList();
      } on MongoDartError catch (error) {
        expect(error.mongoCode, 167);
        expect(error.errorCode, '167');
      } catch (error) {
        expect('$error', 'Should not throw this error');
      }
    }, skip: cannotRunTests);
  });

  tearDownAll(() async {
    await client.connect();
    db = client.db();
    await Future.forEach(usedCollectionNames,
        (String collectionName) => db.collection(collectionName).drop());
    await client.close();
  });
}
