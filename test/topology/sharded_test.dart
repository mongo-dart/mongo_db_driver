import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:test/test.dart';

import '../utils/test_database.dart';

const dbName = 'mongo-dart-sharded';
const dbServerName = 'sharded';
const defaultUri = 'mongodb://127.0.0.1:27017/$dbName';

void main() async {
  var testInfo = await testDatabase(defaultUri);
  var client = testInfo.client;
  if (client == null) {
    return;
  }

  Future cleanupDatabase() async {
    await client.close();
  }

  group('Sharded Cluster Test', () {
    tearDownAll(() async {
      await cleanupDatabase();
    });

    group('Server Description', () {
      if (!testInfo.serverfound || !testInfo.isShardedCluster) {
        return;
      }
      test('Simple', () async {
        var db = client.db();
        var collection = db.collection('sharded-test');
        await collection.drop();

        var serverDescription =
            client.topology?.servers.first.serverDescription;

        expect(serverDescription?.address, '127.0.0.1:27017');
      });
      test('Write and Find', () async {
        var db = client.db();
        var collection = db.collection('sharded-test');
        await collection.drop();

        var serverDescription =
            client.topology?.servers.first.serverDescription;
        expect(serverDescription?.address, '127.0.0.1:27017');

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);
        serverDescription = client.topology?.servers.first.serverDescription;
        expect((serverDescription?.options.roundTripTime ?? 0) > 0, isTrue);
        expect((serverDescription?.options.minRoundTripTime ?? 0) > 0, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        serverDescription = client.topology?.servers.first.serverDescription;
        expect((serverDescription?.options.roundTripTime ?? 0) > 0, isTrue);
        expect((serverDescription?.options.minRoundTripTime ?? 0) > 0, isTrue);
      });
    });
  });
}
