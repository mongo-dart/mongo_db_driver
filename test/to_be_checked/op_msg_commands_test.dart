import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/database/cursor.dart';
import 'package:mongo_db_driver/src/unions/query_union.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

const dbName = 'test-mongo-dart-commands';
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

  group('Commands', () {
    setUp(() async {
      await initializeDatabase();
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    group('Administration Commands', () {
      group('Kill cursor Command:', () {
        test('test on existing cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          await insertManyDocuments(collection, 10000);

          var cursor =
              Cursor(FindOperation(collection, QueryUnion({})), db.server);

          expect(cursor.state, CursorState.init);

          var cursorResult = await cursor.nextObject();
          expect(cursor.state, CursorState.open);
          expect(cursor.cursorId.isNegative, isFalse);
          expect(cursorResult?['a'], 0);
          expect(cursorResult, isNotNull);
          var command = KillCursorsCommand(collection, [cursor.cursorId]);
          var result = await command.process();
          expect(result, isNotNull);
          expect((result[keyCursorsKilled] as List).first, cursor.cursorId);
          expect(result[keyCursorsAlive], isEmpty);
          expect(result[keyCursorsUnknown], isEmpty);
          expect(result[keyCursorsNotFound], isEmpty);
        });
        test('test on small cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);
          var command = KillCursorsCommand(collection, [Int64(1)]);
          var result = await command.process();
          expect(result, isNotNull);
          expect(result[keyOk], 1.0);
          expect((result[keyCursorsNotFound] as List).first, Int64(1));
          expect(result[keyCursorsAlive], isEmpty);
          expect(result[keyCursorsUnknown], isEmpty);
          expect(result[keyCursorsKilled], isEmpty);
        });
        test('test on non existing cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);
          var command = KillCursorsCommand(collection, [Int64(111111111111)]);
          var result = await command.process();
          expect(result, isNotNull);
          expect(result[keyOk], 1.0);
          expect(
              (result[keyCursorsNotFound] as List).first, Int64(111111111111));
          expect(result[keyCursorsAlive], isEmpty);
          expect(result[keyCursorsUnknown], isEmpty);
          expect(result[keyCursorsKilled], isEmpty);
        });
        // The idea is to check if an existing cursor is listend
        // in 'cursorsAlive' element.
        // At present is not listed
        test('test on diff cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);
          await insertManyDocuments(collection, 10000);
          var cursor =
              Cursor(FindOperation(collection, QueryUnion({})), db.server);
          expect(cursor.state, CursorState.init);
          await cursor.nextObject();
          var command = KillCursorsCommand(collection, [Int64(1)]);
          var result = await command.process();
          expect(result, isNotNull);
          expect(result[keyOk], 1.0);
          expect((result[keyCursorsNotFound] as List).first, Int64(1));
          expect(result[keyCursorsAlive], isEmpty);
          expect(result[keyCursorsUnknown], isEmpty);
          expect(result[keyCursorsKilled], isEmpty);
        });

        test('test with return Object on small cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);
          var command = KillCursorsCommand(collection, [Int64(1)]);
          var result = await command.executeDocument(db.server);
          expect(result, isNotNull);
          expect(result.success, isTrue);
          expect(result.cursorsNotFound?.first, Int64(1));
          expect(result.cursorsAlive, isNull);
          expect(result.cursorsUnknown, isNull);
          expect(result.cursorsKilled, isNull);
        });
        test('test with return Object on non existing cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);
          var command = KillCursorsCommand(collection, [Int64(-1)]);
          var result = await command.executeDocument(db.server);
          expect(result, isNotNull);
          expect(result.success, isTrue);
          expect(result.cursorsNotFound?.first, Int64(-1));
          expect(result.cursorsAlive, isNull);
          expect(result.cursorsUnknown, isNull);
          expect(result.cursorsKilled, isNull);
        });
      });
      group('get more Command:', () {
        test('test on existing cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          await insertManyDocuments(collection, 10000);

          var cursor =
              Cursor(FindOperation(collection, QueryUnion({})), db.server);

          expect(cursor.state, CursorState.init);

          var cursorResult = await cursor.nextObject();
          expect(cursor.state, CursorState.open);
          expect(cursor.cursorId.isNegative, isFalse);
          expect(cursorResult?['a'], 0);
          expect(cursorResult, isNotNull);
          var command = GetMoreCommand(collection, cursor.cursorId,
              session: cursor.cursorSession);
          var result = await command.process();
          expect(result, isNotNull);
          expect(result[keyCursor], isNotNull);

          var cursorMap = result[keyCursor] as Map;
          expect(cursorMap[keyFirstBatch], isNull);
          expect(cursorMap[keyNextBatch], isNotEmpty);
          expect(cursorMap[keyNextBatch].length, 101);
        });
        test('test on non existing cursor', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          var command = GetMoreCommand(collection, Int64(1));
          var result = await command.process();
          expect(result, isNotNull);
          expect(result[keyOk], 0.0);
          expect(result[keyCursor], isNull);
          expect((result[keyErrmsg] as String).isNotEmpty, isTrue);
        });
        test('test batch size option', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          await insertManyDocuments(collection, 10000);

          var cursor =
              Cursor(FindOperation(collection, QueryUnion({})), db.server);

          expect(cursor.state, CursorState.init);

          var cursorResult = await cursor.nextObject();
          expect(cursor.state, CursorState.open);
          expect(cursor.cursorId.isNegative, isFalse);
          expect(cursorResult?['a'], 0);
          expect(cursorResult, isNotNull);
          var options = GetMoreOptions(batchSize: 10);
          var command = GetMoreCommand(collection, cursor.cursorId,
              getMoreOptions: options, session: cursor.cursorSession);
          var result = await command.executeDocument();
          expect(result, isNotNull);
          var cursorRes = result.cursorResult;
          expect(cursorRes, isNotNull);
          expect(cursorRes.nextBatch, isNotEmpty);
          expect(cursorRes.nextBatch.length, 10);

          options = GetMoreOptions(batchSize: 200);
          command = GetMoreCommand(collection, cursor.cursorId,
              getMoreOptions: options, session: cursor.cursorSession);
          result = await command.executeDocument();
          expect(result, isNotNull);
          cursorRes = result.cursorResult;
          expect(cursorRes, isNotNull);
          expect(cursorRes.nextBatch.length, 200);
        });
        test('test huge batch size', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          await insertManyDocuments(collection, 10000);

          var cursor = Cursor(
              FindOperation(collection, QueryUnion({}),
                  findOptions: FindOptions(batchSize: 1)),
              db.server);
          await cursor.nextObject();
          var options = GetMoreOptions(batchSize: 10001);
          var command = GetMoreCommand(collection, cursor.cursorId,
              getMoreOptions: options, session: cursor.cursorSession);
          var result = await command.executeDocument();
          expect(result, isNotNull);
          var cursorRes = result.cursorResult;
          expect(cursorRes, isNotNull);
          expect(cursorRes.nextBatch, isNotEmpty);
          expect(cursorRes.nextBatch.length, 9999);
        });
        test('test error', () async {
          var collectionName = getRandomCollectionName();
          var collection = db.collection(collectionName);

          await insertManyDocuments(collection, 10000);

          var cursor =
              Cursor(FindOperation(collection, QueryUnion({})), db.server);
          await cursor.nextObject();

          var command = GetMoreCommand(collection, cursor.cursorId,
              session: cursor.cursorSession);
          var result = await command.executeDocument();
          expect(result, isNotNull);
          var cursorRes = result.cursorResult;
          expect(cursorRes.nextBatch, isNotEmpty);
          expect(cursorRes.nextBatch.length, 101);
          expect(() => GetMoreOptions(batchSize: 0), throwsMongoDartError);
        });
      });

      group('Get All Parameters', () {
        test('Run command', () async {
          var command = GetAllParametersCommand(client);
          var ret = await command.process();
          expect(ret[keyOk], 1.0);
          expect(ret[keyLogLevel], 0);
          if (!db.server.serverCapabilities.isShardedCluster) {
            expect(ret[keyFeatureCompatibilityVersion], isMap);
          }
        });
      });
      group('Get Parameter', () {
        test('Run command', () async {
          var command = GetParameterCommand(client, keyLogLevel);
          var ret = await command.process();
          expect(ret[keyOk], 1.0);
          expect(ret[keyLogLevel], 0);
        });
      });
    });
    group('Diagnostic Commands', () {
      group('Server Status', () {
        test('Map return', () async {
          var command = ServerStatusCommand(client);
          var ret = await command.process();
          expect(ret, isNotNull);
          expect(ret[keyHost], isNotEmpty);
          expect(ret[keyVersion], isNotEmpty);
          expect(ret[keyLatchAnalysis], isNull);
        });
        test('No Host in RawOptions', () async {
          var command = ServerStatusCommand(client, rawOptions: {
            keyHost: 0,
            keyPid: 0,
            keyAsserts: 0,
          });
          var ret = await command.process();
          expect(ret, isNotNull);
          expect(ret[keyHost], isNotNull);
          expect(ret[keyPid], isNotNull);
          expect(ret[keyAsserts], isNull);
        });

        test('No Metric in ServerStatusOptions', () async {
          var command = ServerStatusCommand(client,
              serverStatusOptions: ServerStatusOptions(metricsExcluded: true));
          var ret = await command.process();
          expect(ret, isNotNull);
          expect(ret[keyMetrics], isNull);
        });

        test('No Metric in RawOptions', () async {
          var command =
              ServerStatusCommand(client, rawOptions: {keyMetrics: 0});
          var ret = await command.process();
          expect(ret, isNotNull);
          expect(ret[keyMetrics], isNull);
        });

        test('Only instance values', () async {
          var command = ServerStatusCommand(client,
              serverStatusOptions: ServerStatusOptions.instance);
          var ret = await command.process();
          expect(ret, isNotNull);
          expect(ret[keyMetrics], isNull);
        });

        test('Document return', () async {
          var command = ServerStatusCommand(client);
          var ret = await command.executeDocument(db.server);
          expect(ret, isNotNull);
          expect(ret.host, isNotEmpty);
          expect(ret.localTime?.isAfter(DateTime(2020)), isTrue);
          expect(ret.version, isNotNull);
        });

        test('No Metric in ServerStatusOptions -> Result class', () async {
          var command = ServerStatusCommand(client,
              serverStatusOptions: ServerStatusOptions(metricsExcluded: true));
          var ret = await command.executeDocument(db.server);
          expect(ret, isNotNull);
          expect(ret.metrics, isNull);
        });

        test('No Metric in RawOptions -> Result class', () async {
          var command =
              ServerStatusCommand(client, rawOptions: {keyMetrics: 0});
          var ret = await command.executeDocument(db.server);
          expect(ret, isNotNull);
          expect(ret.metrics, isNull);
        });
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
