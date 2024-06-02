import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/core/error/connection_exception.dart';
import 'package:mongo_db_driver/src/topology/abstract/topology.dart';
import 'package:sasl_scram/sasl_scram.dart';

class DbTestInfo {
  MongoClient? client;
  bool serverfound = false;
  bool isVer3_2 = false;
  bool isVer3_6 = false;
  bool isAuthenticated = false;
  String? fcv;
  bool isStandalone = false;
  bool isReplicaSet = false;
  bool isShardedCluster = false;
}

/// Returned client to be closed in test
Future<DbTestInfo> testDatabase(String uriString,
    {MongoClientOptions? mongoClientOptions}) async {
  var testInfo = DbTestInfo();

  testInfo.client =
      MongoClient(uriString, mongoClientOptions: mongoClientOptions);
  Uri uri = Uri.parse(uriString);
  try {
    await testInfo.client!.connect();
    var db = testInfo.client!.db();
    testInfo.serverfound = true;
    testInfo.isStandalone =
        testInfo.client!.topology!.type == TopologyType.standalone;
    testInfo.isReplicaSet =
        testInfo.client!.topology!.type == TopologyType.replicaSet;
    testInfo.isShardedCluster =
        testInfo.client!.topology!.type == TopologyType.shardedCluster;
    testInfo.fcv = db.server.serverCapabilities.fcv;
    testInfo.isAuthenticated = testInfo.client!.isAuthenticated;
    testInfo.isVer3_2 = testInfo.fcv == '3.2';
    testInfo.isVer3_6 = testInfo.fcv == '3.6';
    //await client.close();
    return testInfo;
  } on Map catch (e) {
    if (e.containsKey(keyCode)) {
      if (e[keyCode] == 18) {
        return testInfo;
      }
    }
    throw StateError('Unknown error $e');
    // When the server is not reachable on the required address (port!?)
  } on SaslScramException catch (e) {
    if (e.message.contains('Username is empty')) {
      return testInfo;
    }
    throw StateError('Unknown error $e');
  } on MongoDartError catch (e) {
    if (e.mongoCode == 18) {
      return testInfo;
    }
    rethrow;
  } on ConnectionException catch (e) {
    if (e.message.contains(':${uri.port}')) {
      return testInfo;
    }
    throw StateError('Unknown error $e');
  } catch (e) {
    throw StateError('Unknown error $e');
  }
}
/* 
Future<String?> getFcv(String uri) async {
  var client = MongoClient(uri);
  try {
    await client.connect();
    var db = client.db();

    var fcv = db.server.serverCapabilities.fcv;

    await client.close();
    return fcv;
  } on Map catch (e) {
    if (e.containsKey(keyCode)) {
      if (e[keyCode] == 18) {
        return null;
      }
    }
    throw StateError('Unknown error $e');
  } catch (e) {
    throw StateError('Unknown error $e');
  }
}
 */
