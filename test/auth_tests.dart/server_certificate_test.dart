// ignore_for_file: unused_local_variable

import 'dart:io';

import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:test/test.dart';

// Run server1 with these parameters:
// mongod --port 27032  --dbpath <your-data-path> --oplogSize 128
//  --tlsMode requireTLS --tlsCertificateKeyFile
//  <your-mongo-dart-folder>/test/certificates_for_testing/server1.pem
//
// Run server2 with these parameters:
// mongod --port 27033  --dbpath <your-data-path-2> --oplogSize 128
//  --tlsMode requireTLS --tlsCertificateKeyFile
//  <your-mongo-dart-folder>/test/certificates_for_testing/server2.pem
const dbName = 'mongo-dart-server-cert';
const dbServerName = 'server1';
const defaultUri = 'mongodb://127.0.0.1:27032/$dbName';

const dbServerName2 = 'server2';
const defaultUri2 = 'mongodb://127.0.0.1:27033/$dbName';

void main() async {
  bool serverfound = false;
  bool isVer3_2 = false;
  bool isVer3_6 = false;
  bool isNoMoreMongodbCR = false;
  var caCertFile =
      File('test/certificates_for_testing/mongo-test-ca-full-chain.crt');

  Future<bool> findServer(String uriString) async {
    var clientOptions = MongoClientOptions()
      ..tls = true
      ..tlsCAFile = caCertFile.path;
    var client = MongoClient(uriString, mongoClientOptions: clientOptions);
    Uri uri = Uri.parse(uriString);
    try {
      await client.connect();
      await client.close();
      return true;
    } on MongoDartError catch (e) {
      if (e.mongoCode == 18) {
        return false;
      }
      rethrow;
    } on Map catch (e) {
      if (e.containsKey(keyCode)) {
        if (e[keyCode] == 18) {
          return false;
        }
      }
      throw StateError('Unknown error $e');
      // When the server is not reachable on the required address (port!?)
    } on ConnectionException catch (e) {
      if (e.message.contains(':${uri.port}')) {
        return false;
      }
      throw StateError('Unknown error $e');
    } catch (e) {
      throw StateError('Unknown error $e');
    }
  }

  Future<String?> getFcv(String uri) async {
    var clientOptions = MongoClientOptions()
      ..tls = true
      ..tlsCAFile = caCertFile.path;
    var client = MongoClient(uri, mongoClientOptions: clientOptions);
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

  serverfound = await findServer(defaultUri);
  if (serverfound) {
    var fcv = await getFcv(defaultUri);
    isVer3_2 = fcv == '3.2';
    isVer3_6 = fcv == '3.6';
    if (fcv != null) {
      isNoMoreMongodbCR = fcv.length != 3 || fcv.compareTo('5.9') == 1;
    }
  }

  group('Server certificate', () {
    if (!serverfound) {
      return;
    }
    test('No certificate, no connection', () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      try {
        await client.connect();
        // If the test has been already run, the certificate
        // already in hash Table
        expect(true, isTrue);
      } on ConnectionException {
        expect(true, isTrue);
      } catch (e) {
        expect(true, isFalse);
      } finally {
        await client.close();
      }
    });

    test('Must be run all together', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      await client.connect();

      await client.close();

      // Check to avoid problems with the
      // "certificate already in hash table error"
      //test('Connect no problems with cert', () async {
      await client.connect();
      await client.close();
      //});

      // same isolate, once connected, the certificate stays in cache
      clientOptions = MongoClientOptions()..tls = true;
      client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      await client.connect();
      await client.close();

      await client.connect();
      await client.close();

      // The certificate stays in cache even for a different server
      // (with a certificate from the same authority)
      clientOptions = MongoClientOptions()..tls = true;
      client = MongoClient(defaultUri2, mongoClientOptions: clientOptions);
      await client.connect();
      await client.close();
    });
  });
}
