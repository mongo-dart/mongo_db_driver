@Timeout(Duration(seconds: 100))
library;

import 'package:bson/bson.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
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

  Future cleanupDatabase() async {
    await client.close();
  }

  group('Commands', () {
    var cannotRunTests = false;

    setUpAll(() async {
      await initializeDatabase();
      if (!db.server.serverCapabilities.supportsOpMsg) {
        cannotRunTests = true;
      }
    });

    tearDown(() async {
      //await cleanupDatabase();
    });
    tearDownAll(() async {
      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
    });
    group('Uuid:', () {
      test('read Uuid', () async {
        if (cannotRunTests) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var uuidValue = Uuid().v4obj();
        await collection.insertOne({
          'standard': LegacyUuid.fromHexString(uuidValue.uuid),
          'javaLegacy': LegacyUuid.toJavaLegacy(uuidValue),
          'cSharpLegacy': LegacyUuid.toCSharpLegacy(uuidValue),
          'pythonLegacy': LegacyUuid.toPythonLegacy(uuidValue)
        });

        var values = await collection
            .find(
                filter: where
                  ..$eq('javaLegacy', LegacyUuid.toJavaLegacy(uuidValue)))
            .toList();

        expect(values.length, 1);
        expect(
            (values.first['standard'] as LegacyUuid).pythonLegacy, uuidValue);
        expect(
            (values.first['javaLegacy'] as LegacyUuid).javaLegacy, uuidValue);
        expect((values.first['cSharpLegacy'] as LegacyUuid).cSharpLegacy,
            uuidValue);
        expect((values.first['pythonLegacy'] as LegacyUuid).pythonLegacy,
            uuidValue);
      });

      test('update Uuid', () async {
        if (cannotRunTests) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var uuidValue = Uuid().v4obj();
        await collection.insertOne({
          'standard': LegacyUuid.fromHexString(uuidValue.uuid),
          'javaLegacy': LegacyUuid.toJavaLegacy(uuidValue),
          'cSharpLegacy': LegacyUuid.toCSharpLegacy(uuidValue),
          'pythonLegacy': LegacyUuid.toPythonLegacy(uuidValue)
        });

        var uuidValue2 = Uuid().v4obj();
        await collection.updateOne(
            where..$eq('cSharpLegacy', LegacyUuid.toCSharpLegacy(uuidValue)),
            modify..$set('javaLegacy2', LegacyUuid.toJavaLegacy(uuidValue2)));

        var values = await collection.find().toList();

        expect(values.length, 1);
        expect(
            (values.first['standard'] as LegacyUuid).pythonLegacy, uuidValue);
        expect(
            (values.first['javaLegacy'] as LegacyUuid).javaLegacy, uuidValue);
        expect(
            (values.first['javaLegacy2'] as LegacyUuid).javaLegacy, uuidValue2);
        expect((values.first['cSharpLegacy'] as LegacyUuid).cSharpLegacy,
            uuidValue);
        expect((values.first['pythonLegacy'] as LegacyUuid).pythonLegacy,
            uuidValue);
      });

      test('replace Legacy Uuid', () async {
        if (cannotRunTests) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);
        var uuidValue = Uuid().v4obj();
        await collection.insertOne({
          'standard': LegacyUuid.fromHexString(uuidValue.uuid),
          'javaLegacy': LegacyUuid.toJavaLegacy(uuidValue),
          'cSharpLegacy': LegacyUuid.toCSharpLegacy(uuidValue),
          'pythonLegacy': LegacyUuid.toPythonLegacy(uuidValue)
        });

        var uuidNew = Uuid().v4obj();
        await collection.replaceOne(
            where..$eq('cSharpLegacy', LegacyUuid.toCSharpLegacy(uuidValue)),
            {
              'cSharpLegacy': LegacyUuid.toCSharpLegacy(uuidValue),
              'pythonLegacy': LegacyUuid.toPythonLegacy(uuidNew)
            },
            upsert: true);

        var values = await collection.find().toList();

        expect(values.length, 1);
        expect((values.first['cSharpLegacy'] as LegacyUuid).cSharpLegacy,
            uuidValue);
        expect(
            (values.first['pythonLegacy'] as LegacyUuid).pythonLegacy, uuidNew);
      });

      test('delete Uuid', () async {
        if (cannotRunTests) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);
        var uuid = Uuid().v4obj();
        await collection.insertOne({
          'standard': LegacyUuid.fromHexString(uuid.uuid),
          'javaLegacy': LegacyUuid.toJavaLegacy(uuid),
          'cSharpLegacy': LegacyUuid.toCSharpLegacy(uuid),
          'pythonLegacy': LegacyUuid.toPythonLegacy(uuid)
        });
        await collection.deleteOne(
            where..$eq('pythonLegacy', LegacyUuid.toPythonLegacy(uuid)));

        var values = await collection.find().toList();

        expect(values.isEmpty, isTrue);
      });
    });
  });
}
