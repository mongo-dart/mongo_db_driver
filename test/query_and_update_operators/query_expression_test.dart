@Timeout(Duration(minutes: 10))
library;

import 'package:mongo_db_driver/mongo_db_driver.dart';
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
  String? fcv;

  group('Query Expression', () {
    setUpAll(() async {
      client = MongoClient(defaultUri);
      db = await initializeDatabase(client);
      fcv = client.topology?.getServer().serverCapabilities.fcv;
    });

    tearDownAll(() async {
      //db = client.db();
      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
    });

    // https://www.mongodb.com/docs/manual/reference/operator/query/regex/
    group('\$regex', () {
      MongoCollection? collection;
      var expectedMultiline = [
        {
          '_id': 100,
          'sku': 'abc123',
          'description': 'Single line description.'
        },
        {'_id': 101, 'sku': 'abc789', 'description': 'First line\nSecond line'},
        {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
      ];
      var expectedNoMultiline = [
        {
          '_id': 100,
          'sku': 'abc123',
          'description': 'Single line description.'
        },
        {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
      ];
      var expectedNoMultilineNoAnchor = [
        {
          '_id': 100,
          'sku': 'abc123',
          'description': 'Single line description.'
        },
        {'_id': 101, 'sku': 'abc789', 'description': 'First line\nSecond line'},
        {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
      ];
      var expDotMatchNewLine = [
        {
          '_id': 102,
          'sku': 'xyz456',
          'description': 'Many spaces before     line'
        },
        {
          '_id': 103,
          'sku': 'xyz789',
          'description': 'Multiple\nline description'
        }
      ];
      var expDotMatchNewLineNos = [
        {
          '_id': 102,
          'sku': 'xyz456',
          'description': 'Many spaces before     line'
        }
      ];
      var expIgnoreSpaceAndComments = [
        {'_id': 100, 'sku': 'abc123', 'description': 'Single line description.'}
      ];
      var expExtendedRegularExpression = [
        {
          '_id': 100,
          'sku': 'abc123',
          'description': 'Single line description.'
        },
        {'_id': 101, 'sku': 'abc789', 'description': 'First line\nSecond line'},
        {'_id': 104, 'sku': 'Abc789', 'description': 'SKU starts with A'}
      ];
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

      test('Document - multiline', () async {
        var findList = await collection!.find(filter: {
          'description': {r'$regex': r'^S', op$options: 'm'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, expectedMultiline);
      });
      test('FilterExpression - multiline', () async {
        var findList = await collection!
            .find(
                filter: FilterExpression()
                  ..$regex('description', r'^S', multiLineAnchorMatch: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expectedMultiline);
      });
      test('QueryExpression - multiline', () async {
        var findList = await collection!
            .find(
                filter: where
                  ..$regex('description', r'^S', multiLineAnchorMatch: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expectedMultiline);
      });
      test('QueryExpression - no multiline', () async {
        var findList = await collection!
            .find(filter: where..$regex('description', r'^S'))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expectedNoMultiline);
      });
      test('QueryExpression - no multiline no Anchor', () async {
        var findList = await collection!
            .find(filter: where..$regex('description', r'S'))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expectedNoMultilineNoAnchor);
      });

      test('Document - Dot Character to Match New Line', () async {
        var findList = await collection!.find(filter: {
          'description': {r'$regex': r'm.*line', op$options: 'si'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, expDotMatchNewLine);
      });
      test('FilterExpression - Dot Character to Match New Line', () async {
        var findList = await collection!
            .find(
                filter: FilterExpression()
                  ..$regex('description', r'm.*line',
                      caseInsensitive: true, dotMatchAll: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expDotMatchNewLine);
      });
      test('QueryExpression - Dot Character to Match New Line', () async {
        var findList = await collection!
            .find(
                filter: where
                  ..$regex('description', r'm.*line',
                      caseInsensitive: true, dotMatchAll: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expDotMatchNewLine);
      });
      test('QueryExpression - Dot Character to Match New Line No DotMatchAll',
          () async {
        var findList = await collection!
            .find(
                filter: where
                  ..$regex('description', r'm.*line', caseInsensitive: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expDotMatchNewLineNos);
      });

      test('Document - Ignore spaces and comments', () async {
        if ((fcv?.compareTo('6.0') ?? -1) < 1) {
          return;
        }
        var pattern = 'abc #category code\n123 #item number';
        var findList = await collection!.find(filter: {
          'sku': {r'$regex': pattern, op$options: 'x'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, expIgnoreSpaceAndComments);
      });
      test('FilterExpression - Ignore spaces and comments', () async {
        if ((fcv?.compareTo('6.0') ?? -1) < 1) {
          return;
        }
        var pattern = 'abc #category code\n123 #item number';
        var findList = await collection!
            .find(
                filter: FilterExpression()
                  ..$regex('sku', pattern, extendedIgnoreWhiteSpace: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expIgnoreSpaceAndComments);
      });
      test('QueryExpression - Ignore spaces and comments', () async {
        if ((fcv?.compareTo('6.0') ?? -1) < 1) {
          return;
        }
        var pattern = 'abc #category code\n123 #item number';
        var findList = await collection!
            .find(
                filter: where
                  ..$regex('sku', pattern, extendedIgnoreWhiteSpace: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expIgnoreSpaceAndComments);
      });

      test('Document - Extended Regular Expression', () async {
        var pattern = "(?i)a(?-i)bc";
        var findList = await collection!.find(filter: {
          'sku': {r'$regex': pattern, op$options: 'x'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, expExtendedRegularExpression);
      });
      test('FilterExpression - Extended Regular Expression', () async {
        var pattern = "(?i)a(?-i)bc";
        var findList = await collection!
            .find(
                filter: FilterExpression()
                  ..$regex('sku', pattern, extendedIgnoreWhiteSpace: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expExtendedRegularExpression);
      });
      test('QueryExpression - Extended Regular Expression', () async {
        var pattern = "(?i)a(?-i)bc";
        var findList = await collection!
            .find(
                filter: where
                  ..$regex('sku', pattern, extendedIgnoreWhiteSpace: true))
            .toList();

        expect(findList, isNotNull);
        expect(findList, expExtendedRegularExpression);
      });

      test('Document - Unicode', () async {
        var collectionName2 = getRandomCollectionName(usedCollectionNames);
        var collection2 = db.collection(collectionName2);
        var (_, MongoDocument result, _, _) = await collection2.insertMany([
          {'_id': 0, 'artist': 'Blue Öyster Cult', "title": 'The Reaper'},
          {'_id': 1, 'artist': 'Blue Öyster Cult', 'title': 'Godzilla'},
          {'_id': 2, 'artist': 'Blue Oyster Cult', 'title': 'Take Me Away'}
        ]);
        if (result[keyOk] != 1.0) {
          throw StateError(result[keyErrmsg]);
        }

        var pattern = "(*UCP)\byster";
        var findList = await collection2.find(filter: {
          'artist': {r'$regex': pattern, op$options: 'x'}
        }).toList();

        expect(findList, isNotNull);
        expect(findList, []);
      });
    });
  });
}
