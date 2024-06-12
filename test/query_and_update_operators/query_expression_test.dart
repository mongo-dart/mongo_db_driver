@Timeout(Duration(minutes: 10))
library;

import 'package:mongo_db_driver/mongo_db_driver.dart' hide MongoDocument;
import 'package:mongo_db_query/mongo_db_query.dart';
import 'dart:async';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test-mongo-dart-query-expression';
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

Future cleanupDatabase(MongoClient client) async {
  await client.close();
}

void main() async {
  late MongoClient client;
  late MongoDatabase db;
  List<String> usedCollectionNames = [];

  group('Query Expression', () {
    setUpAll(() async {
      client = MongoClient(defaultUri);
      db = await initializeDatabase(client);
    });

    tearDownAll(() async {
      //db = client.db();
      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
    });

    // https://www.mongodb.com/docs/manual/reference/operator/query/regex/
    // TODO complete examples
    group('\$regex', () {
      MongoCollection? collection;
      setUpAll(() async {
        var collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName);
        var (_, MongoDocument result, _, _) = await collection!.insertMany([
          {
            '_id': 100,
            'sku': "abc123",
            'description': "Single line description."
          },
          {
            '_id': 101,
            'sku': "abc789",
            'description': "First line\nSecond line"
          },
          {
            '_id': 102,
            'sku': "xyz456",
            'description': "Many spaces before     line"
          },
          {
            '_id': 103,
            'sku': "xyz789",
            'description': "Multiple\nline description"
          },
          {'_id': 104, 'sku': "Abc789", 'description': "SKU starts with A"}
        ]);
        if (result[keyOk] != 1.0) {
          throw StateError(result[keyErrmsg]);
        }
      });
      test('Document - simple', () async {
        var findList = await collection!.find(filter: {
          'sku': {op$regex: r'789$'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {
            '_id': 103,
            'sku': 'xyz789',
            'description': 'Multiple\nline description'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });
      test('FilterExpression - simple', () async {
        var findList = await collection!
            .find(filter: FilterExpression()..$regex('sku', r'789$'))
            .toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {
            '_id': 103,
            'sku': 'xyz789',
            'description': 'Multiple\nline description'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });
      test('QueryExpression - simple', () async {
        var findList = await collection!
            .find(filter: where..$regex('sku', r'789$'))
            .toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {
            '_id': 103,
            'sku': 'xyz789',
            'description': 'Multiple\nline description'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });

      test('Document - case insensitive', () async {
        var findList = await collection!.find(filter: {
          'sku': {r'$regex': r'^ABC', op$options: 'i'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 100,
            'sku': 'abc123',
            'description': 'Single line description.'
          },
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });
      test('FilterExpression - case insensitive', () async {
        var findList = await collection!
            .find(
                filter: FilterExpression()
                  ..$regex('sku', r'^ABC', caseInsensitive: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 100,
            'sku': 'abc123',
            'description': 'Single line description.'
          },
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });
      test('QueryExpression - case insensitive', () async {
        var findList = await collection!
            .find(filter: where..$regex('sku', r'^ABC', caseInsensitive: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, [
          {
            '_id': 100,
            'sku': 'abc123',
            'description': 'Single line description.'
          },
          {
            '_id': 101,
            'sku': 'abc789',
            'description': 'First line\nSecond line'
          },
          {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
        ]);
      });
    });
  });
}
