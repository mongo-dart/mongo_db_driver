import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/core/auth/scram_sha1_authenticator.dart';
import 'package:mongo_db_driver/src/core/auth/scram_sha256_authenticator.dart';
import 'package:sasl_scram/sasl_scram.dart' show CryptoStrengthStringGenerator;
import 'package:test/test.dart';

import '../utils/test_database.dart';
//final String mongoDbUri =
//    'mongodb://test:test@ds031477.mlab.com:31477/dart';

//  switch (serverType) {
//    case ServerType.simple:
//      defaultPort = '27017';
//      defaultPort2 = defaultPort;
//    case ServerType.simpleAuth:
//      defaultPort = '27031';
//      defaultPort2 = defaultPort;
//    case ServerType.tlsServer:
//      defaultPort = '27032';
//      defaultPort2 = '27033';
//    case ServerType.tlsServerAuth:
//      defaultPort = '27034';
//      defaultPort2 = '27035';
//    case ServerType.tlsClient:
//      defaultPort = '27036';
//      defaultPort2 = defaultPort;
//    case ServerType.tlsClientAuth:
//      defaultPort = '27037';
//      defaultPort2 = defaultPort;
//    case ServerType.x509Auth:
//      defaultPort = '27038';
//      defaultPort2 = defaultPort;
//  }

const dbName = 'mongodb-auth';
const dbAddress = '127.0.0.1';

const mongoDbUri = 'mongodb://test:test@$dbAddress:27031/$dbName';
const mongoDbUri2 = 'mongodb://unicode:übelkübel@$dbAddress:27031/$dbName';
const mongoDbUri3 = 'mongodb://special:1234AbcD##@$dbAddress:27031/$dbName';

void main() async {
  /*  Future<String?> getFcv(String uri) async {
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
  group('Authentication', () {
    var serverRequiresAuth = false;
    var isVer3_6 = false;
    var isVer3_2 = false;
    DbTestInfo? dbTestInfo;

    setUpAll(() async {
      dbTestInfo = await testDatabase(mongoDbUri);
      serverRequiresAuth = dbTestInfo!.isAuthenticated;
      var fcv = dbTestInfo!.fcv;
      if (serverRequiresAuth) {
        isVer3_2 = fcv == '3.2';
        isVer3_6 = fcv == '3.6';
      }
    });

    group('Basic', () {
      test('uri.parse', () {
        var connectionUri =
            Uri.parse('mongodb://@127.0.0.1:27031/mongodb-auth');
        expect(connectionUri.userInfo, '');
      });

      test('uri.parse 2', () {
        var connectionUri =
            Uri.parse('mongodb://unicode:übelkübel@$dbAddress:27031/$dbName');
        expect(connectionUri.userInfo, 'unicode:%C3%BCbelk%C3%BCbel');
      });
      test('uri.parse 3', () {
        var connectionUri =
            Uri.parse('mongodb://special:1234AbcD@$dbAddress:27031/$dbName');
        expect(connectionUri.userInfo, 'special:1234AbcD');
      });
      test('uri.parse 4', () {
        var connectionUri = Uri.parse(
            'mongodb://special:1234A%00%23bcD@$dbAddress:27031/$dbName');
        expect(connectionUri.userInfo, 'special:1234A%00%23bcD');
      });
      test('uri.parse 5', () {
        var connectionUri =
            Uri.parse('mongodb://special:1234A##bcD@$dbAddress:27031/$dbName');
        expect(connectionUri.userInfo, 'special:1234AbcD');
      });
      test('uri.parse dbUri3', () {
        var connectionUri = Uri.parse(mongoDbUri3);
        expect(connectionUri.userInfo, 'user');
      });
    });

    group('General Test', () {
      //if (!serverRequiresAuth) {
      //  return;
      //}

      test('Should be able to connect and authenticate', () async {
        if (serverRequiresAuth) {
          var client = MongoClient(mongoDbUri);
          await client.connect();
          var db = client.db();

          await db.collection('test').find().toList();
          await client.close();
        }
      });
      test('Should be able to connect and authenticate with scram sha1',
          () async {
        if (serverRequiresAuth) {
          var client = MongoClient(
              '$mongoDbUri?authMechanism=${ScramSha1Authenticator.name}');
          await client.connect();
          var db = client.db();

          expect(db.server.isAuthenticated, isTrue);
          await db.collection('test').find().toList();
          await client.close();
        }
      });
      test('Should be able to connect and authenticate with scram sha256',
          () async {
        if (serverRequiresAuth && !isVer3_6 && !isVer3_2) {
          var client = MongoClient(
              '$mongoDbUri?authMechanism=${ScramSha256Authenticator.name}');
          await client.connect();
          var db = client.db();

          expect(db.server.isAuthenticated, isTrue);
          await db.collection('test').find().toList();
          await client.close();
          client = MongoClient(
              '$mongoDbUri2?authMechanism=${ScramSha256Authenticator.name}');
          await client.connect();
          db = client.db();

          expect(db.server.isAuthenticated, isTrue);
          await db.collection('test').find().toList();
          await client.close();
        }
      });
      test(
          'Should be able to connect and authenticate special with scram sha256',
          () async {
        if (serverRequiresAuth && !isVer3_6 && !isVer3_2) {
          var client = MongoClient(
              '$mongoDbUri?authMechanism=${ScramSha256Authenticator.name}');
          await client.connect();
          var db = client.db();

          expect(db.server.isAuthenticated, isTrue);
          await db.collection('test').find().toList();
          await client.close();
          client = MongoClient(
              '$mongoDbUri3?authMechanism=${ScramSha256Authenticator.name}');
          await client.connect();
          db = client.db();

          expect(db.server.isAuthenticated, isTrue);
          await db.collection('test').find().toList();
          await client.close();
        }
      });
      test("Throw exception when auth mechanism isn't supported", () async {
        if (serverRequiresAuth) {
          final authMechanism = 'Anything';
          var client = MongoClient('$mongoDbUri?authMechanism=$authMechanism');

          dynamic sut() async => await client.connect();

          expect(
              sut(),
              throwsA(predicate((MongoDartError e) =>
                  e.message ==
                  'Provided authentication scheme is not supported : $authMechanism')));
        }
      });
    });

    group('RandomStringGenerator', () {
      test("Shouldn't produce twice the same string", () {
        var generator = CryptoStrengthStringGenerator();

        var results = {};

        for (var i = 0; i < 100000; ++i) {
          var generatedString = generator.generate(20);
          if (results.containsKey(generatedString)) {
            fail("Shouldn't have generated 2 identical strings");
          } else {
            results[generatedString] = 1;
          }
        }
      });
    });
  });
}
