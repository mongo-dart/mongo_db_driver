@Timeout(Duration(seconds: 400))
library;

import 'dart:io';

import 'package:basic_utils/basic_utils.dart' show DnsUtils, RRecordType;
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/utils/decode_dns_seed_list.dart';
import 'package:test/test.dart';

import '../utils/throws_utils.dart';

const sslDbConnectionString =
    'mongodb://cluster0-shard-00-00-smeth.gcp.mongodb.net:27017/'
    'test?authSource=admin,'
    'mongodb://cluster0-shard-00-01-smeth.gcp.mongodb.net:27017/'
    'test?authSource=admin,'
    'mongodb://cluster0-shard-00-02-smeth.gcp.mongodb.net:27017/'
    'test?authSource=admin';
const sslDbUsername = 'mongo_dart_tester';
const sslDbPassword = 'O8kipHnIyenpc9fV';
const sslQueryParmConnectionString =
    'mongodb://cluster0-shard-00-00-smeth.gcp.mongodb.net:27017,'
    'cluster0-shard-00-01-smeth.gcp.mongodb.net:27017,'
    'cluster0-shard-00-02-smeth.gcp.mongodb.net:27017/'
    'test?authSource=admin&ssl=true';
// Todo manage also the case in which the server is not the primary
const tlsQueryParmConnectionString = 'mongodb://cluster0-shard-00-01-smeth'
    '.gcp.mongodb.net:27017/test?tls=true&authSource=admin';
const atlasConnectionString = 'mongodb+srv://user:pwd@address.mongodb.net/'
    'test?authMechanism=SCRAM-SHA-256&retryWrites=true&w=majority';
const doConnectionString = 'mongodb+srv://user:pwd@'
    'db-mongodb-address.mongo.ondigitalocean.com/'
    'test?authSource=admin&replicaSet=db-mongodb-test&tls=true'
    '&tlsCAFile=/home/cert-path&authMechanism=SCRAM-SHA-1';

void main() {
  Map<String, String> envVars = Platform.environment;
  var atlasDomain = envVars['ATLAS_DOMAIN'] ?? '';
  var atlasUser = envVars['ATLAS_USER'];
  var atlasPwd = envVars['ATLAS_PWD'];
  var atlasApp = envVars['ATLAS_APP'];

  if (atlasDomain.isEmpty) {
    throw StateError('Atlas Environment variables missing');
  }

  var atlasUrl = 'mongodb+srv://$atlasUser:$atlasPwd@$atlasDomain/test'
      '?retryWrites=true&w=majority&appName=$atlasApp';

  group('Dns lookup', () {
    test('Testing connection TXT', () async {
      var records = await DnsUtils.lookupRecord(atlasDomain, RRecordType.TXT);
      expect(records?.first.data,
          'authSource=admin&replicaSet=atlas-jajzr3-shard-0');
    });
    test('Testing connection SRV', () async {
      var records = await DnsUtils.lookupRecord(
          '_mongodb._tcp.$atlasDomain', RRecordType.SRV);

      expect(records?.first.data.startsWith('0 0 27017'), isTrue);
      expect(records?.first.data.endsWith('.mongodb.net.'), isTrue);
      expect(records?[1].data.startsWith('0 0 27017'), isTrue);
      expect(records?[1].data.endsWith('.mongodb.net.'), isTrue);
      expect(records?.last.data.startsWith('0 0 27017'), isTrue);
      expect(records?.last.data.endsWith('.mongodb.net.'), isTrue);
    });

    test('Decode Dns Seedlist', () async {
      var result = await decodeDnsSeedlist(Uri.parse(atlasUrl));

      expect(
          result.first.startsWith('mongodb://$atlasUser:$atlasPwd@'), isTrue);
      expect(
          result.first
              .contains('.mongodb.net:27017/test?authSource=admin&replicaSet='),
          isTrue);
      expect(
          result.first.endsWith(
              '&retryWrites=true&w=majority&appName=$atlasApp&ssl=true'),
          isTrue);

      expect(result[1].startsWith('mongodb://$atlasUser:$atlasPwd@'), isTrue);
      expect(
          result[1]
              .contains('.mongodb.net:27017/test?authSource=admin&replicaSet='),
          isTrue);
      expect(
          result[1].endsWith(
              '&retryWrites=true&w=majority&appName=$atlasApp&ssl=true'),
          isTrue);

      expect(result.last.startsWith('mongodb://$atlasUser:$atlasPwd@'), isTrue);
      expect(
          result.last
              .contains('.mongodb.net:27017/test?authSource=admin&replicaSet='),
          isTrue);
      expect(
          result.last.endsWith(
              '&retryWrites=true&w=majority&appName=$atlasApp&ssl=true'),
          isTrue);
    });
    test('Decode Dns Seedlist - sync', () {
      decodeDnsSeedlist(Uri.parse(atlasUrl)).then((result) {
        expect(
            result.first,
            'mongodb://user:password@rs1.joedrumgoole.com:27022/'
            'test?authSource=admin&replicaSet=srvdemo&'
            'retryWrites=true&w=majority&ssl=true');
        expect(
            result[1],
            'mongodb://user:password@rs2.joedrumgoole.com:27022/'
            'test?authSource=admin&replicaSet=srvdemo&'
            'retryWrites=true&w=majority&ssl=true');
        expect(
            result.last,
            'mongodb://user:password@rs3.joedrumgoole.com:27022/'
            'test?authSource=admin&replicaSet=srvdemo&'
            'retryWrites=true&w=majority&ssl=true');
      });
    });
    test('Decode Dns Seedlist - Wrong host error', () async {
      expect(
          () async => decodeDnsSeedlist(Uri.parse('mongodb+srv://user:password@'
              'rsx.joedrumgoole.com/test?retryWrites=true&w=majority')),
          throwsMongoDartError);
    });
    test('Decode Dns Seedlist - More than one host error', () async {
      expect(
          () async => decodeDnsSeedlist(Uri.parse('mongodb+srv://user:password@'
              'rs.joedrumgoole.com, rs2.joedrumgoole.com/'
              'test?retryWrites=true&w=majority')),
          throwsMongoDartError);
    });
    test('Db creation with seedlist format url', () async {
      var client = MongoClient(atlasUrl);
      await client.connect();
      var db = client.db();

      var urilist = db.uriList;
      expect(
          urilist.first.startsWith('mongodb://$atlasUser:$atlasPwd@'), isTrue);
      expect(
          urilist[1]
              .contains('.mongodb.net:27017/test?authSource=admin&replicaSet='),
          isTrue);
      expect(
          urilist.last.endsWith(
              '&retryWrites=true&w=majority&appName=$atlasApp&ssl=true'),
          isTrue);
    });
    test('Test Atlas connection', () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var client = MongoClient(atlasUrl, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();

      var coll = db.collection('test-insert');
      /* var result = */
      await coll.insertOne({'solved': true, 'autoinit': 'delayed'});
      // Todo update test
      // print(result['ops'].first);
      /* Todo update
      var findResult = await coll.find(where.id(result['insertedId'])).toList();
      print(findResult);
      expect(result['ops'].first['solved'], findResult.first['solved']);
      expect(result['ops'].first['autoinit'], findResult.first['autoinit']); */
      await client.close();
    });
    test('Test Atlas connection performance', () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var t0 = DateTime.now().millisecondsSinceEpoch;
      var client = MongoClient(atlasUrl, mongoClientOptions: clientOptions);
      var t1 = DateTime.now().millisecondsSinceEpoch;
      print('Client: ${t1 - t0}');
      await client.connect();
      var t2 = DateTime.now().millisecondsSinceEpoch;
      print('Connect: ${t2 - t1}');
      print('Total: ${t2 - t0}');
      await client.close();
    });
    test('Test DigitalOcean connection', () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var client =
          MongoClient(doConnectionString, mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();
      var coll = db.collection('test-insert');
      /*     var result = */
      await coll.insertOne({'solved': true, 'autoinit': 'delayed'});
      // Todo update test
      // print(result['ops'].first);
      /* Todo update
      var findResult = await coll.find(where.id(result['insertedId'])).toList();
      print(findResult);
      expect(result['ops'].first['solved'], findResult.first['solved']);
      expect(result['ops'].first['autoinit'], findResult.first['autoinit']); */
      await client.close();
    },
        skip: 'Set the correct Digita Ocean connection string '
            'before running this test');
  });

  group('Real connection', () {
    // TODO, check pool
    /*  test('Connect and authenticate to a database over SSL', () async {
        var clientOptions = MongoClientOptions()..tls = true;
      var client =
          MongoClient(sslDbConnectionString.split(','), mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();
      //var db = Db.pool(sslDbConnectionString.split(','));
    
      await db.authenticate(sslDbUsername, sslDbPassword);
      await db.collection('test').find().toList();
      await client.close();
    }); */

    test('Ssl as query parm', () async {
      var client = MongoClient(sslQueryParmConnectionString);
      await client.connect();
      var db = client.db();

      await client.authenticate(sslDbUsername, sslDbPassword);
      await db.collection('test').find().toList();
      await client.close();
    });

    // TODO Check Pool
    /*  test('Ssl with no secure info => Error', () async {
      var client = MongoClient(sslDbConnectionString.split(','));
      await client.connect();
      expect(() => client.connect(), throwsA((ConnectionException e) => true));
    }); */

    test('Tls as query parm', () async {
      var client = MongoClient(tlsQueryParmConnectionString);
      await client.connect();
      var db = client.db();

      await client.authenticate(sslDbUsername, sslDbPassword);
      await db.collection('test').find().toList();
      await client.close();
    });
    test('Tls as query parm plus secure parameter', () async {
      var clientOptions = MongoClientOptions()..tls = true;
      var client = MongoClient(tlsQueryParmConnectionString,
          mongoClientOptions: clientOptions);
      await client.connect();
      var db = client.db();

      await client.authenticate(sslDbUsername, sslDbPassword);
      await db.collection('test').find().toList();
      await client.close();
    });
  }, skip: 'Requires manual connection string adjustment before run');
}
