// ignore_for_file: deprecated_member_use_from_same_package

@Timeout(Duration(minutes: 2))
library;

import 'package:bson/bson.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/client/client_exp.dart';
import 'package:mongo_db_driver/src/topology/abstract/topology.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';
import 'dart:async';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import '../utils/test_database.dart';

const dbName = 'test-mongo-dart-read-preference';

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

Future cleanupDatabase(MongoClient client) async => await client.close();

void main() async {
  late MongoDatabase db;
  List<String> usedCollectionNames = [];
  var testInfo = await testDatabase(defaultUri);
  var client = testInfo.client;
  if (client == null) {
    return;
  }

  group('Read Preference', () {
    setUpAll(() async {
      db = await initializeDatabase(client);
    });

    tearDownAll(() async {
      await Future.delayed(Duration(seconds: 1));

      await Future.forEach(usedCollectionNames,
          (String collectionName) => db.collection(collectionName).drop());
      await client.close();
    });

    // https://www.mongodb.com/docs/manual/core/read-preference/
    group('Class construction', () {
      MongoCollection? collection;

      setUp(() async {
        var collectionName = getRandomCollectionName(usedCollectionNames);
        collection = db.collection(collectionName);
        await collection!.insertMany([
          {
            '_id': ObjectId.parse('52769ea0f3dc6ead47c9a1b2'),
            'author': "abc123",
            'title': "zzz",
            'tags': ["programming", "database", "mongodb"]
          }
        ]);
      });

      test('primary', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.primary);

        expect(readPreference, ReadPreference.primary);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, isNull);
      });

      /// [see](https://www.mongodb.com/docs/manual/reference/method/Mongo.setReadPref/#parameters)
      test('primary + tagSet', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.primary, tags: [
                  {'region': 'South', 'datacenter': 'A'},
                  {'rack': 'rack-1'},
                  {}
                ]),
            throwsArgumentError);
      });
      test('primary + maxStalenessSeconds', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.primary,
                maxStalenessSeconds: 90),
            throwsArgumentError);
      });

      /// Optional. A document that specifies whether to enable the use of hedged reads:
      ///
      /// { enabled: <boolean> }
      ///
      /// The enabled field defaults to true; i.e. specifying an empty document
      /// { } is equivalent to specifying { enabled: true }.
      test('primary + hedgeOptions empty', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.primary, hedgeOptions: {}),
            throwsArgumentError);
      });
      test('primary + hedgeOptions', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.primary,
                hedgeOptions: {'enabled': true}),
            throwsArgumentError);
      });
      test('secondary', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.secondary);

        expect(readPreference, ReadPreference.secondary);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, isNull);
      });
      test('secondary + tagSet', () async {
        var readPreference =
            ReadPreference(ReadPreferenceMode.secondary, tags: [
          {'region': 'South', 'datacenter': 'A'},
          {'rack': 'rack-1'},
          {}
        ]);

        expect(readPreference, ReadPreference.secondary);
        expect(readPreference.tags, [
          {'region': 'South', 'datacenter': 'A'},
          {'rack': 'rack-1'},
          {}
        ]);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, isNull);

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyTags: readPreference.tags
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                    keyTags: readPreference.tags
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });
      test('secondary + maxStalenessSeconds -error', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.primary,
                maxStalenessSeconds: 60),
            throwsArgumentError);
      });
      test('secondary + maxStalenessSeconds', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.secondary,
            maxStalenessSeconds: 90);

        expect(readPreference, ReadPreference.secondary);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, 90);
        expect(readPreference.hedgeOptions, isNull);

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyMaxStalenessSecond: readPreference.maxStalenessSeconds
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                    keyMaxStalenessSecond: readPreference.maxStalenessSeconds
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });

      /// Optional. A document that specifies whether to enable the use of hedged reads:
      ///
      /// { enabled: <boolean> }
      ///
      /// The enabled field defaults to true; i.e. specifying an empty document
      /// { } is equivalent to specifying { enabled: true }.
      test('secondary + hedgeOptions empty', () async {
        var readPreference =
            ReadPreference(ReadPreferenceMode.secondary, hedgeOptions: {});

        expect(readPreference, ReadPreference.secondary);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyHedgeOptions: readPreference.hedgeOptions
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });
      test('secondary + hedgeOptions', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.secondary,
            hedgeOptions: {'enabled': true});

        expect(readPreference, ReadPreference.secondary);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {'enabled': true});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyHedgeOptions: readPreference.hedgeOptions
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });

      test('nearest', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.nearest);

        expect(readPreference, ReadPreference.nearest);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {});
      });
      test('nearest from static ', () async {
        expect(ReadPreference.nearest, ReadPreference.nearest);
        expect(ReadPreference.nearest.tags, isNull);
        expect(ReadPreference.nearest.maxStalenessSeconds, isNull);
        expect(ReadPreference.nearest.hedgeOptions, {'enabled': true});
      });
      test('nearest + tagSet', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.nearest, tags: [
          {'region': 'South', 'datacenter': 'A'},
          {'rack': 'rack-1'},
          {}
        ]);

        expect(readPreference, ReadPreference.nearest);
        expect(readPreference.tags, [
          {'region': 'South', 'datacenter': 'A'},
          {'rack': 'rack-1'},
          {}
        ]);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyTags: readPreference.tags
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                    keyTags: readPreference.tags
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });
      test('nearest + maxStalenessSeconds -error', () async {
        expect(
            () => ReadPreference(ReadPreferenceMode.nearest,
                maxStalenessSeconds: 60),
            throwsArgumentError);
      });
      test('nearest + maxStalenessSeconds', () async {
        var readPreference =
            ReadPreference(ReadPreferenceMode.nearest, maxStalenessSeconds: 90);

        expect(readPreference, ReadPreference.nearest);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, 90);
        expect(readPreference.hedgeOptions, {});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyMaxStalenessSecond: readPreference.maxStalenessSeconds
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                    keyMaxStalenessSecond: readPreference.maxStalenessSeconds
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });

      /// Optional. A document that specifies whether to enable the use of hedged reads:
      ///
      /// { enabled: <boolean> }
      ///
      /// The enabled field defaults to true; i.e. specifying an empty document
      /// { } is equivalent to specifying { enabled: true }.
      test('nearest + hedgeOptions empty', () async {
        var readPreference =
            ReadPreference(ReadPreferenceMode.nearest, hedgeOptions: {});

        expect(readPreference, ReadPreference.nearest);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyHedgeOptions: readPreference.hedgeOptions
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });
      test('nearest + hedgeOptions', () async {
        var readPreference = ReadPreference(ReadPreferenceMode.nearest,
            hedgeOptions: {'enabled': true});

        expect(readPreference, ReadPreference.nearest);
        expect(readPreference.tags, isNull);
        expect(readPreference.maxStalenessSeconds, isNull);
        expect(readPreference.hedgeOptions, {'enabled': true});

        if (testInfo.isShardedCluster) {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {
                  keyMode: readPreference.mode.toString(),
                  keyHedgeOptions: readPreference.hedgeOptions
                }
              });
        } else if (testInfo.isReplicaSet) {
          if ((client.topology?.type ?? TopologyType.unknown) ==
              TopologyType.single) {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {keyMode: 'primary'}
                });
          } else {
            expect(
                readPreference.toMap(
                    topologyType:
                        client.topology?.type ?? TopologyType.unknown),
                {
                  key$ReadPreference: {
                    keyMode: readPreference.mode.name,
                  }
                });
          }
        } else {
          expect(
              readPreference.toMap(
                  topologyType: client.topology?.type ?? TopologyType.unknown),
              {
                key$ReadPreference: {keyMode: 'primary'}
              });
        }
      });
    });
  });
}
