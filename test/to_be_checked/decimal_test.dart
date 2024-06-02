@Timeout(Duration(seconds: 100))
library;

import 'package:mongo_db_driver/mongo_dart_old.dart';
import 'package:decimal/decimal.dart';
import 'package:mongo_db_driver/src/database/base/mongo_database.dart';
import 'package:mongo_db_driver/src/database/base/mongo_collection.dart';
import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

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
    await collection.deleteMany();
    var toInsert = <Map<String, dynamic>>[];
    for (var n = 0; n < numberOfRecords; n++) {
      toInsert.add({'a': Decimal.fromInt(n)});
    }

    await collection.insertMany(toInsert);
  }

  Future cleanupDatabase() async {
    await client.close();
  }

  group('Commands', () {
    setUp(() async {
      await initializeDatabase();
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    group('Decimal:', () {
      test('read Decimal', () async {
        var collectionName = getRandomCollectionName();

        var collection = db.collection(collectionName);

        await insertManyDocuments(collection, 10000);

        var values = await collection.find().toList();

        expect(values.length, 10000);

        expect(values.last['a'], Decimal.fromInt(9999));

        var ra = Decimal.parse('999999999999999999999999999999');
        for (var idx = 0; idx < 10; idx++) {
          var value = values[idx];
          var update = (value['a'] as Decimal) + ra;
          //print(update);
          await collection.updateOne(<String, dynamic>{
            '_id': value['_id']
          }, <String, dynamic>{
            r'$set': {'r': update}
          });
        }
      });
      test('update Decimal', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        await collection.insertOne(
            <String, dynamic>{'value': Decimal.fromInt(3), 'qty': 4});

        await collection.updateOne(<String, dynamic>{}, <String, dynamic>{
          r'$mul': {'value': Decimal.fromInt(5), 'qty': 5}
        });

        var values = await collection.find().toList();

        expect(values.length, 1);
        expect(values.first['value'], Decimal.fromInt(15));
        expect(values.first['qty'], 20);

        await collection.updateOne(
            where,
            UpdateExpression()
              ..$mul('value', Decimal.fromInt(5))
              ..$mul('qty', 2));
        values = await collection.find().toList();

        expect(values.length, 1);
        expect(values.first['value'], Decimal.fromInt(75));
        expect(values.first['qty'], 40);
      });
      test('update Decimal with Modifier', () async {
        var collectionName = getRandomCollectionName();

        var collection = db.collection(collectionName);

        await collection.insertOne(
            <String, dynamic>{'value': Decimal.fromInt(3), 'qty': 4});

        var mody = UpdateExpression()
          ..$inc('value', Decimal.fromInt(2))
          ..$inc('qty', 3);

        await collection.updateOne(<String, dynamic>{}, mody.build());

        var values = await collection.find().toList();

        expect(values.length, 1);

        expect(values.last['value'], Decimal.fromInt(5));
        expect(values.last['qty'], 7);
      });
    });
  });

  tearDownAll(() async {
    await client.connect();
    db = client.db();
    await Future.forEach(usedCollectionNames,
        (String collectionName) => db.collection(collectionName).drop());
    await client.close();
  });
}
