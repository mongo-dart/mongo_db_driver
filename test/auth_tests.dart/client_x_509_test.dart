// ignore_for_file: unused_local_variable

@Timeout(Duration(minutes: 2))
library;

import 'dart:io';

import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:test/test.dart';

import '../utils/throws_utils.dart';

// Run server1 with these parameters:
// mongod --port 27038  --dbpath <your-data-path> --oplogSize 128
//  --tlsMode requireTLS --tlsCertificateKeyFile
//  <your-mongo-dart-folder>/test/certificates_for_testing/server1.pem
//  --tlsCAFile
//  <your-mongo-dart-folder>/test/certificates_for_testing/mongo-test-ca-full-chain.crt
//
const dbName = 'mongodb-auth';
const defaultUri =
    'mongodb://127.0.0.1:27038/$dbName?authMechanism=MONGODB-X509';
const lateAuthUri = 'mongodb://127.0.0.1:27038/$dbName';

void main() async {
  bool serverfound = false;
  bool isVer3_2 = false;
  bool isVer3_6 = false;
  bool isNoMoreMongodbCR = false;
  var caCertFile = File('${Directory.current.path}'
      '/test/certificates_for_testing/mongo-test-ca-full-chain.crt');
  var pemFile = File('${Directory.current.path}'
      '/test/certificates_for_testing/client.mongo.pem');
  var wrongPemFile = File('test/certificates_for_testing/client.mongo.crt');

  Future<MongoClient?> findServer(String uriString) async {
    var clientOptions = MongoClientOptions()
      ..tls = true
      ..tlsCAFile = caCertFile.path
      ..tlsCertificateKeyFile = pemFile.path;
    var client = MongoClient(uriString, mongoClientOptions: clientOptions);
    Uri uri = Uri.parse(uriString);
    try {
      await client.connect();

      return client;
      //await client.close();
      //return true;
    } on MongoDartError catch (e) {
      if (e.mongoCode == 18) {
        return null;
      }
      rethrow;
    } on Map catch (e) {
      if (e.containsKey(keyCode)) {
        if (e[keyCode] == 18) {
          return null;
        }
      }
      throw StateError('Unknown error $e');
      // When the server is not reachable on the required address (port!?)
    } on ConnectionException catch (e) {
      if (e.message.contains(':${uri.port}')) {
        return null;
      }
      throw StateError('Unknown error $e');
    } catch (e) {
      throw StateError('Unknown error $e');
    }
  }

  Future<String?> getFcv(MongoClient client /* String uri */) async {
    /*   var clientOptions = MongoClientOptions()
      ..tls = true
      ..tlsCAFile = caCertFile.path
      ..tlsCertificateKeyFile = pemFile.path;
    var client = MongoClient(uri, mongoClientOptions: clientOptions);
    try {
      await client.connect(); */
    var db = client.db();
    var fcv = db.server.serverCapabilities.fcv;

    //await client.close();
    return fcv;
    /*  } on Map catch (e) {
      if (e.containsKey(keyCode)) {
        if (e[keyCode] == 18) {
          return null;
        }
      }
      throw StateError('Unknown error $e');
    } catch (e) {
      throw StateError('Unknown error $e');
    } */
  }

  var client = await findServer(defaultUri);
  if (client != null) {
    var fcv = await getFcv(client /* defaultUri */);
    isVer3_2 = fcv == '3.2';
    isVer3_6 = fcv == '3.6';
    if (fcv != null) {
      isNoMoreMongodbCR = fcv.length != 3 || fcv.compareTo('5.9') == 1;
    }
    await client.close();
  }

  group('Client certificate', () {
    if (client == null) {
      return;
    }

    test('Should not be able to connect missing key file and CA file',
        () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      try {
        await client.connect();
        // after the first run certificates are in the hash table.
        expect(true, isTrue);
      } on ConnectionException {
        expect(true, isTrue);
      } catch (e) {
        expect(true, isFalse);
      } finally {
        await client.close();
      }
    });

    test('Should not be able to connect missing key file', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      try {
        await client.connect();

        // after the first run certificates are in the hash table.
        expect(true, isTrue);
      } on ConnectionException {
        expect(true, isTrue);
      } catch (e) {
        expect(true, isFalse);
      } finally {
        await client.close();
      }
    });

    // Check to avoid problems with the
    // "certificate already in hash table error"
    test(
        'Should not be able to connect missing key file, CA File given 2 times',
        () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      try {
        await client.connect();

        // after the first run certificates are in the hash table.
        expect(true, isTrue);
      } on ConnectionException {
        expect(true, isTrue);
      } catch (e) {
        expect(true, isFalse);
      } finally {
        await client.close();
      }
    });

    test('Wrong pem file', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = wrongPemFile.path;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      try {
        await client.connect();

        await client.close();
        expect(true, isFalse);
      } on ConnectionException {
        expect(true, isTrue);
      } catch (e) {
        expect(true, isFalse);
      } finally {
        await client.close();
      }
    });
    test('Connect no problems with cert', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = pemFile.path
        ..connectionPoolSettings = (ConnectionPoolSettings()..minPoolSize = 1);
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();
      expect(client.isAuthenticated, isTrue);

      var collection = db.collection('test');
      await collection.find().toList();

      await client.close();
    });
    test('Connect no problems with cert - 2 connections', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = pemFile.path
        ..connectionPoolSettings = (ConnectionPoolSettings()..minPoolSize = 2);
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();
      expect(db.server.isAuthenticated, isTrue);

      var collection = db.collection('test');
      await collection.find().toList();

      await client.close();
    });

    test('Reopen connection', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = pemFile.path;
      var client = MongoClient(defaultUri, mongoClientOptions: clientOptions);
      await client.connect();
      await client.close();

      await client.connect();
      await client.close();
    });
    test('Connect & Authenticate - Simple find() - Error', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = pemFile.path;
      var client = MongoClient(lateAuthUri, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();

      var collection = db.collection('test');
      expect(() => collection.find().toList(), throwsMongoDartError);
    });
    test('Connect & Authenticate - Simple find()', () async {
      var clientOptions = MongoClientOptions()
        ..tls = true
        ..tlsCAFile = caCertFile.path
        ..tlsCertificateKeyFile = pemFile.path;
      var client = MongoClient(lateAuthUri, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();

      var collection = db.collection('test');
      await client.authenticateX509();
      expect(db.server.isAuthenticated, isTrue);
      await collection.find().toList();
      await client.close();
    });
  });
}
