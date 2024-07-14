import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import '../utils/insert_data.dart';

const dbName = 'test-mongo-dart-update';
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

  Future cleanupDatabase() async {
    await client.close();
  }

  group('Helper functions', () {
    test('Check update document', () {
      var testDocument = <String, dynamic>{
        r'$set': {'violations': 3},
        r'$unset': {'status': ''}
      };

      expect(UpdateSpec(testDocument).containsOnlyUpdateOperators, isTrue);
      expect(UpdateSpec(testDocument).isPureDocument, isFalse);
    });
    test('Check replace document', () {
      var testDocument = <String, dynamic>{
        'violations': 3,
        'status': 'A',
        'name': 'Ethan'
      };

      expect(UpdateSpec(testDocument).containsOnlyUpdateOperators, isFalse);
      expect(UpdateSpec(testDocument).isPureDocument, isTrue);
    });
    /*   test('Check null document', () {
      expect(containsOnlyUpdateOperators(null), isFalse);
      expect(isPureDocument(null), isFalse);
    }); */
    test('Check empty document', () {
      var testDocument = <String, dynamic>{};
      expect(UpdateSpec(testDocument).containsOnlyUpdateOperators, isFalse);
      expect(UpdateSpec(testDocument).isPureDocument, isTrue);
    });
  });

  group('Update Operations', () {
    //var cannotRunTests = false;
    //var running4_4orGreater = false;
    var running4_2orGreater = false;
    var running4_2 = false;

    //var isReplicaSet = false;
    //var isStandalone = false;
    var isSharded = false;
    setUp(() async {
      await initializeDatabase();
      if (!db.server.serverCapabilities.supportsOpMsg) {
        //cannotRunTests = true;
      }
      var serverFcv = db.server.serverCapabilities.fcv;
      if (serverFcv?.compareTo('4.4') != -1) {
        //running4_4orGreater = true;
      }
      if (serverFcv?.compareTo('4.2') != -1) {
        running4_2orGreater = true;
      }
      if (serverFcv?.compareTo('4.2') == 0) {
        running4_2 = true;
      }
      //isReplicaSet = db.server.serverCapabilities.isReplicaSet;
      //isStandalone = db.server.serverCapabilities.isStandalone;
      isSharded = db.server.serverCapabilities.isShardedCluster;
    });

    tearDown(() async {
      await cleanupDatabase();
    });

    group('Update', () {
      test('Update specific fields on one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'Pending',
            'points': 0,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{'member': 'abc123'}),
                  UpdateUnion(<String, Object>{
                    r'$set': {'status': 'A'},
                    r'$inc': {'points': 1}
                  }))
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 1);
        expect(res[keyNModified], 1);
      });
      test('Update specific fields on multiple documents', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{}),
                  UpdateUnion(<String, Object>{
                    r'$set': {'status': 'A'},
                    r'$inc': {'points': 1}
                  }),
                  multi: true)
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 2);
        expect(res[keyNModified], 2);
      });

      test('Update document on multiple documents - error', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(QueryUnion(<String, Object>{}),
                  UpdateUnion(<String, Object>{'status': 'A', 'points': 1}),
                  multi: true)
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 0);
        expect(res[keyNModified], 0);
        if (isSharded && running4_2) {
          expect((res[keyWriteErrors] as List).first['code'], 72);
        } else {
          expect((res[keyWriteErrors] as List).first['code'], 9);
        }
      });

      test('Replace one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {'_id': 1, 'name': 'Central Perk Cafe', 'Borough': 'Manhattan'},
          {
            '_id': 2,
            'name': 'Rock A Feller Bar and Grill',
            'Borough': 'Queens',
            'violations': 2
          },
          {
            '_id': 3,
            'name': 'Empire State Pub',
            'Borough': 'Brooklyn',
            'violations': 0
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{'name': 'Central Perk Cafe'}),
                  UpdateUnion(<String, Object>{
                    'name': 'Central Park Cafe',
                    'Borough': 'Manhattan'
                  }))
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 1);
        expect(res[keyNModified], 1);
      });

      test('Update with Aggregation Pipeline', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 2,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'A',
            'points': 60,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{}),
                  UpdateUnion([
                    {
                      r'$set': {
                        'status': 'Modified',
                        'comments': [r'$misc1', r'$misc2']
                      }
                    },
                    {
                      r'$unset': ['misc1', 'misc2']
                    }
                  ]),
                  multi: true)
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 2);
        expect(res[keyNModified], 2);
      });

      test('Update with Aggregation Pipeline - 2', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'tests': [95, 92, 90]
          },
          {
            '_id': 2,
            'tests': [94, 88, 90]
          },
          {
            '_id': 3,
            'tests': [70, 75, 82]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{}),
                  UpdateUnion([
                    {
                      r'$set': {
                        'average': {r'$avg': r'$tests'}
                      }
                    },
                    {
                      r'$set': {
                        'grade': {
                          r'$switch': {
                            'branches': [
                              {
                                'case': {
                                  r'$gte': [r'$average', 90]
                                },
                                'then': 'A'
                              },
                              {
                                'case': {
                                  r'$gte': [r'$average', 80]
                                },
                                'then': 'B'
                              },
                              {
                                'case': {
                                  r'$gte': [r'$average', 70]
                                },
                                'then': 'C'
                              },
                              {
                                'case': {
                                  r'$gte': [r'$average', 60]
                                },
                                'then': 'D'
                              }
                            ],
                            'default': 'F'
                          }
                        }
                      }
                    }
                  ]),
                  multi: true)
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 3);
        expect(res[keyNModified], 3);
      });

      test('Bulk Update', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertMembers(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
            collection,
            [
              UpdateStatement(
                  QueryUnion(<String, Object>{'status': 'P'}),
                  UpdateUnion({
                    r'$set': {
                      'status': 'D',
                    }
                  }),
                  multi: true),
              UpdateStatement(QueryUnion(<String, Object>{'_id': 7}),
                  UpdateUnion({'_id': 7, 'name': 'abc123', 'status': 'A'}),
                  upsert: true)
            ],
            ordered: false,
            updateOptions: UpdateOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 4);
        expect(res[keyNModified], 3);
        expect(res[keyUpserted], isNotNull);
        expect(res[keyUpserted], isNotEmpty);
        expect((res[keyUpserted] as List).first['index'], 1);
        expect((res[keyUpserted] as List).first['_id'], 7);
      });

      test('Specify collation', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertFrenchCafe(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
          collection,
          [
            UpdateStatement(
                QueryUnion(<String, Object>{'category': 'cafe', 'status': 'a'}),
                UpdateUnion(<String, Object>{
                  r'$set': {'status': 'Updated'},
                }),
                collation: CollationOptions('fr', strength: 1))
          ],
        );
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 1);
        expect(res[keyNModified], 1);
      });
      test('Array filters - Update elements Match arrayFilters Criteria',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [95, 92, 90]
          },
          {
            '_id': 2,
            'grades': [98, 100, 102]
          },
          {
            '_id': 3,
            'grades': [95, 110, 100]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
          collection,
          [
            UpdateStatement(
                QueryUnion(<String, Object>{
                  'grades': {r'$gte': 100}
                }),
                UpdateUnion(<String, Object>{
                  r'$set': {r'grades.$[element]': 100},
                }),
                arrayFilters: [
                  {
                    'element': {r'$gte': 100}
                  }
                ],
                multi: true)
          ],
        );
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 2);
        expect(res[keyNModified], 2);
      });
      test('Array filters - Update Specific Elements of an Array of Documents',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [
              {'grade': 80, 'mean': 75, 'std': 6},
              {'grade': 85, 'mean': 90, 'std': 4},
              {'grade': 85, 'mean': 85, 'std': 6}
            ]
          },
          {
            '_id': 2,
            'grades': [
              {'grade': 90, 'mean': 75, 'std': 6},
              {'grade': 87, 'mean': 90, 'std': 3},
              {'grade': 85, 'mean': 85, 'std': 4}
            ]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOperation = UpdateOperation(
          collection,
          [
            UpdateStatement(
                QueryUnion(<String, Object>{}),
                UpdateUnion(<String, Object>{
                  r'$set': {r'grades.$[element].mean': 100},
                }),
                arrayFilters: [
                  {
                    'element.grade': {r'$gte': 85}
                  }
                ],
                multi: true)
          ],
        );
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 2);
        expect(res[keyNModified], 2);
      });
      test('Specify hint', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertMembers(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        await collection.createIndex(keys: {'status': 1});
        await collection.createIndex(key: 'points');

        var updateOperation = UpdateOperation(
          collection,
          [
            UpdateStatement(
                QueryUnion(<String, Object>{
                  'points': {r'$lte': 20},
                  'status': 'P'
                }),
                UpdateUnion(<String, Object>{
                  r'$set': {'misc1': 'Need to activate'},
                }),
                hint: HintUnion(<String, Object>{'status': 1}),
                multi: true)
          ],
        );
        var res = await updateOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 3);
        expect(res[keyNModified], 3);
      });
    });
    group('Update - wrapper', () {
      test('Update specific fields on one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'Pending',
            'points': 0,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOneOperation = UpdateOneOperation(
            collection,
            UpdateOneStatement(
                QueryUnion(<String, Object>{'member': 'abc123'}),
                UpdateUnion(<String, Map<String, dynamic>>{
                  r'$set': {'status': 'A'},
                  r'$inc': {'points': 1}
                })),
            updateOneOptions: UpdateOneOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var (res, _) = await updateOneOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 1);
        expect(res.nMatched, 1);
      });

      test('Update specific fields on multiple documents', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateManyOperation = UpdateManyOperation(
            collection,
            UpdateManyStatement(
                QueryUnion(<String, Object>{}),
                UpdateUnion(<String, Map<String, dynamic>>{
                  r'$set': {'status': 'A'},
                  r'$inc': {'points': 1}
                })),
            updateManyOptions: UpdateManyOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);
      });
      test('Replace one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {'_id': 1, 'name': 'Central Perk Cafe', 'Borough': 'Manhattan'},
          {
            '_id': 2,
            'name': 'Rock A Feller Bar and Grill',
            'Borough': 'Queens',
            'violations': 2
          },
          {
            '_id': 3,
            'name': 'Empire State Pub',
            'Borough': 'Brooklyn',
            'violations': 0
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var replaceOneOperation = ReplaceOneOperation(
            collection,
            ReplaceOneStatement(
                QueryUnion(<String, Object>{'name': 'Central Perk Cafe'}),
                <String, Object>{
                  'name': 'Central Park Cafe',
                  'Borough': 'Manhattan'
                }));
        var res = await replaceOneOperation.process();

        expect(res, isNotNull);
        expect(res[keyOk], 1.0);
        expect(res[keyN], 1);
        expect(res[keyNModified], 1);
      });
      test('Update with Aggregation Pipeline', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 2,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'A',
            'points': 60,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateManyOperation = UpdateManyOperation(
            collection,
            UpdateManyStatement(
                QueryUnion(<String, Object>{}),
                UpdateUnion([
                  {
                    r'$set': {
                      'status': 'Modified',
                      'comments': [r'$misc1', r'$misc2']
                    }
                  },
                  {
                    r'$unset': ['misc1', 'misc2']
                  }
                ])),
            ordered: false,
            updateManyOptions: UpdateManyOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);
      });

      test('Update with Aggregation Pipeline - 2', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'tests': [95, 92, 90]
          },
          {
            '_id': 2,
            'tests': [94, 88, 90]
          },
          {
            '_id': 3,
            'tests': [70, 75, 82]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateManyOperation = UpdateManyOperation(
            collection,
            UpdateManyStatement(
                QueryUnion(<String, Object>{}),
                UpdateUnion([
                  {
                    r'$set': {
                      'average': {r'$avg': r'$tests'}
                    }
                  },
                  {
                    r'$set': {
                      'grade': {
                        r'$switch': {
                          'branches': [
                            {
                              'case': {
                                r'$gte': [r'$average', 90]
                              },
                              'then': 'A'
                            },
                            {
                              'case': {
                                r'$gte': [r'$average', 80]
                              },
                              'then': 'B'
                            },
                            {
                              'case': {
                                r'$gte': [r'$average', 70]
                              },
                              'then': 'C'
                            },
                            {
                              'case': {
                                r'$gte': [r'$average', 60]
                              },
                              'then': 'D'
                            }
                          ],
                          'default': 'F'
                        }
                      }
                    }
                  }
                ])),
            ordered: false,
            updateManyOptions: UpdateManyOptions(
                writeConcern: WriteConcern(w: wMajority, wtimeout: 5000)));
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);
      });

      test('Specify collation', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertFrenchCafe(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateOneOperation = UpdateOneOperation(
          collection,
          UpdateOneStatement(
              QueryUnion(<String, Object>{'category': 'cafe', 'status': 'a'}),
              UpdateUnion(<String, Map<String, dynamic>>{
                r'$set': {'status': 'Updated'},
              }),
              collation: CollationOptions('fr', strength: 1)),
        );
        var (res, _) = await updateOneOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);
      });
      test('Array filters - Update elements Match arrayFilters Criteria',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [95, 92, 90]
          },
          {
            '_id': 2,
            'grades': [98, 100, 102]
          },
          {
            '_id': 3,
            'grades': [95, 110, 100]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateManyOperation = UpdateManyOperation(
          collection,
          UpdateManyStatement(
              QueryUnion(<String, Object>{
                'grades': {r'$gte': 100}
              }),
              UpdateUnion(<String, Map<String, dynamic>>{
                r'$set': {r'grades.$[element]': 100},
              }),
              arrayFilters: [
                {
                  'element': {r'$gte': 100}
                }
              ]),
        );
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);
      });
      test('Array filters - Update Specific Elements of an Array of Documents',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [
              {'grade': 80, 'mean': 75, 'std': 6},
              {'grade': 85, 'mean': 90, 'std': 4},
              {'grade': 85, 'mean': 85, 'std': 6}
            ]
          },
          {
            '_id': 2,
            'grades': [
              {'grade': 90, 'mean': 75, 'std': 6},
              {'grade': 87, 'mean': 90, 'std': 3},
              {'grade': 85, 'mean': 85, 'std': 4}
            ]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var updateManyOperation = UpdateManyOperation(
          collection,
          UpdateManyStatement(
              QueryUnion(<String, Object>{}),
              UpdateUnion(<String, Map<String, dynamic>>{
                r'$set': {r'grades.$[element].mean': 100},
              }),
              arrayFilters: [
                {
                  'element.grade': {r'$gte': 85}
                }
              ]),
        );
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);
      });
      test('Specify hint', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertMembers(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        await collection.createIndex(keys: {'status': 1});
        await collection.createIndex(key: 'points');

        var updateManyOperation = UpdateManyOperation(
          collection,
          UpdateManyStatement(
              QueryUnion(<String, Object>{
                'points': {r'$lte': 20},
                'status': 'P'
              }),
              UpdateUnion(<String, Map<String, dynamic>>{
                r'$set': {'misc1': 'Need to activate'},
              }),
              hint: HintUnion(<String, Object>{'status': 1})),
        );
        var (res, _) = await updateManyOperation.executeDocument();

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);
      });
    });

    group('Update - Modern Collection helper', () {
      test('Update specific fields on one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'Pending',
            'points': 0,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateOne(
            where..$eq('member', 'abc123'),
            UpdateExpression()
              ..$set('status', 'A')
              ..$inc('points', 1),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);

        var elements = await collection
            .find(filter: where..$eq('member', 'abc123'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 1);
      });
      test('Update specific fields on one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'Pending',
            'points': 0,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateOne(
            where..$eq('member', 'abc123'),
            UpdateExpression()
              ..$set('status', 'A')
              ..$inc('points', 1),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);

        var elements = await collection
            .find(filter: where..$eq('member', 'abc123'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 1);
      });
      test('Update specific fields on multiple documents', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            where,
            UpdateExpression()
              ..$set('status', 'A')
              ..$inc('points', 1),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find(filter: where).toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 2);
        expect(elements.last['status'], 'A');
        expect(elements.last['points'], 60);
      });
      test('Update with Aggregation Pipeline', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 2,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'A',
            'points': 60,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            null,
            (AggregationPipelineBuilder()
                  ..addStage($set.raw({
                    'status': 'Modified',
                    'comments': [r'$misc1', r'$misc2']
                  }))
                  ..addStage($unset(['misc1', 'misc2'])))
                .build(),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find(filter: where).toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'Modified');
        expect(elements.first['points'], 2);
        expect(elements.last['status'], 'Modified');
        expect(elements.last['points'], 60);
      });

      test('Update with Aggregation Pipeline - 2', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'tests': [95, 92, 90]
          },
          {
            '_id': 2,
            'tests': [94, 88, 90]
          },
          {
            '_id': 3,
            'tests': [70, 75, 82]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            null,
            (AggregationPipelineBuilder()
                  ..addStage($set.raw({
                    'average': {r'$avg': r'$tests'}
                  }))
                  ..addStage($set.raw({
                    'grade': {
                      r'$switch': {
                        'branches': [
                          {
                            'case': {
                              r'$gte': [r'$average', 90]
                            },
                            'then': 'A'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 80]
                            },
                            'then': 'B'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 70]
                            },
                            'then': 'C'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 60]
                            },
                            'then': 'D'
                          }
                        ],
                        'default': 'F'
                      }
                    }
                  })))
                .build(),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);

        var elements = await collection.find(filter: where).toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['average'], 92.33333333333333);
        expect(elements.first['grade'], 'A');
        expect(elements.last['average'], 75.66666666666667);
        expect(elements.last['grade'], 'C');
      });

      test('Specify collation', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertFrenchCafe(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateOne(
            where
              ..$eq('category', 'cafe')
              ..$eq('status', 'a'),
            UpdateExpression()..$set('status', 'Updated'),
            collation: CollationOptions('fr', strength: 1));

        expect(res, isNotNull);
        expect(res.isSuccess, true);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);

        var elements = await collection
            .find(filter: where..$eq('status', 'Updated'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 1);
        expect(elements.first['status'], 'Updated');
        expect(elements.first['category'], 'café');
      });
      test('Array filters - Update elements Match arrayFilters Criteria',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [95, 92, 90]
          },
          {
            '_id': 2,
            'grades': [98, 100, 102]
          },
          {
            '_id': 3,
            'grades': [95, 110, 100]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(where..$gte('grades', 100),
            UpdateExpression()..$set(r'grades.$[element]', 100),
            arrayFilters: [
              {
                'element': {r'$gte': 100}
              }
            ]);

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['grades'].first, 95);
        expect(elements.last['grades'].last, 100);
      });
      test('Array filters - Update Specific Elements of an Array of Documents',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [
              {'grade': 80, 'mean': 75, 'std': 6},
              {'grade': 85, 'mean': 90, 'std': 4},
              {'grade': 85, 'mean': 85, 'std': 6}
            ]
          },
          {
            '_id': 2,
            'grades': [
              {'grade': 90, 'mean': 75, 'std': 6},
              {'grade': 87, 'mean': 90, 'std': 3},
              {'grade': 85, 'mean': 85, 'std': 4}
            ]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            where, UpdateExpression()..$set(r'grades.$[element].mean', 100),
            arrayFilters: [
              {
                'element.grade': {r'$gte': 85}
              }
            ]);

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 2);
        expect(elements.first['grades'].last['mean'], 100);
        expect(elements.last['grades'].first['mean'], 100);
      });
      test('Specify hint', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertMembers(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        await collection.createIndex(keys: {'status': 1});
        await collection.createIndex(key: 'points');

        var (res, _) = await collection.updateMany(
            where
              ..$lte('points', 20)
              ..$eq('status', 'P'),
            UpdateExpression()..$set('misc1', 'Need to activate'),
            hint: HintUnion(<String, Object>{'status': 1}));

        expect(res, isNotNull);
        expect(res.isSuccess, isTrue);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);

        var elements =
            await collection.find(filter: where..$eq('status', 'P')).toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['misc1'], 'Need to activate');
        expect(elements.last['misc1'], 'Need to activate');
      });
    });
    group('Update - Collection helpers updateOne, updateMany and replaceOne',
        () {
      test(' Update specific fields on one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'Pending',
            'points': 0,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateOne(
            where..$eq('member', 'abc123'),
            UpdateExpression()
              ..$set('status', 'A')
              ..$inc('points', 1),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 1);
        expect(res.nMatched, 1);

        var elements = await collection
            .find(filter: where..$eq('member', 'abc123'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 1);
      });
      test('Update one document - error', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {'_id': 1, 'name': 'Central Perk Cafe', 'Borough': 'Manhattan'},
          {
            '_id': 2,
            'name': 'Rock A Feller Bar and Grill',
            'Borough': 'Queens',
            'violations': 2
          },
          {
            '_id': 3,
            'name': 'Empire State Pub',
            'Borough': 'Brooklyn',
            'violations': 0
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        expect(
            () => collection.updateOne(
                    where..$eq('name', 'Central Perk Cafe'), <String, Object>{
                  'name': 'Central Park Cafe',
                  'Borough': 'Manhattan'
                }),
            throwsMongoDartError);
      });

      test('Update specific fields on multiple documents', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            where,
            UpdateExpression()
              ..$set('status', 'A')
              ..$inc('points', 1),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 2);
        expect(elements.last['status'], 'A');
        expect(elements.last['points'], 60);
      });
      test('Set new document on multiple documents - error', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 1,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'D',
            'points': 59,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            where,
            {
              'member': 'delete_this',
              'status': '1',
              'points': 0,
            },
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 0);
        expect(res.nModified, 0);
        expect(res.isFailure, isTrue);
        expect(res.hasWriteErrors, isTrue);
        if (isSharded && running4_2) {
          expect(res.writeError?.code, 72);
        } else {
          expect(res.writeError?.code, 9);
        }

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'A');
        expect(elements.first['points'], 1);
        expect(elements.last['status'], 'D');
        expect(elements.last['points'], 59);
      });

      test('Replace one document', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {'_id': 1, 'name': 'Central Perk Cafe', 'Borough': 'Manhattan'},
          {
            '_id': 2,
            'name': 'Rock A Feller Bar and Grill',
            'Borough': 'Queens',
            'violations': 2
          },
          {
            '_id': 3,
            'name': 'Empire State Pub',
            'Borough': 'Brooklyn',
            'violations': 0
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.replaceOne(
            where..$eq('name', 'Central Perk Cafe'), <String, Object>{
          'name': 'Central Park Cafe',
          'Borough': 'Manhattan'
        });

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);

        var elements = await collection
            .find(filter: where..$eq('name', 'Central Park Cafe'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 1);
        expect(elements.first['_id'], 1);
        expect(elements.first['Borough'], 'Manhattan');
        expect(elements.first['violations'], null);
      });

      test('Replace one - document with update operators - error', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {'_id': 1, 'name': 'Central Perk Cafe', 'Borough': 'Manhattan'},
          {
            '_id': 2,
            'name': 'Rock A Feller Bar and Grill',
            'Borough': 'Queens',
            'violations': 2
          },
          {
            '_id': 3,
            'name': 'Empire State Pub',
            'Borough': 'Brooklyn',
            'violations': 0
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        expect(
            () =>
                collection.replaceOne(where..$eq('name', 'Central Perk Cafe'), {
                  r'$set': {'name', 'Central Park Cafe'}
                }),
            throwsMongoDartError);
      });

      test('Update with Aggregation Pipeline', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'member': 'abc123',
            'status': 'A',
            'points': 2,
            'misc1': 'note to self: confirm status',
            'misc2': 'Need to activate'
          },
          {
            '_id': 2,
            'member': 'xyz123',
            'status': 'A',
            'points': 60,
            'misc1': 'reminder: ping me at 100pts',
            'misc2': 'Some random comment'
          },
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            null,
            (AggregationPipelineBuilder()
                  ..addStage($set.raw({
                    'status': 'Modified',
                    'comments': [r'$misc1', r'$misc2']
                  }))
                  ..addStage($unset(['misc1', 'misc2'])))
                .build(),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.first['status'], 'Modified');
        expect(elements.first['points'], 2);
        expect(elements.last['status'], 'Modified');
        expect(elements.last['points'], 60);
      });

      test('Update with Aggregation Pipeline - 2', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'tests': [95, 92, 90]
          },
          {
            '_id': 2,
            'tests': [94, 88, 90]
          },
          {
            '_id': 3,
            'tests': [70, 75, 82]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            null,
            (AggregationPipelineBuilder()
                  ..addStage($set.raw({
                    'average': {r'$avg': r'$tests'}
                  }))
                  ..addStage($set.raw({
                    'grade': {
                      r'$switch': {
                        'branches': [
                          {
                            'case': {
                              r'$gte': [r'$average', 90]
                            },
                            'then': 'A'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 80]
                            },
                            'then': 'B'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 70]
                            },
                            'then': 'C'
                          },
                          {
                            'case': {
                              r'$gte': [r'$average', 60]
                            },
                            'then': 'D'
                          }
                        ],
                        'default': 'F'
                      }
                    }
                  })))
                .build(),
            writeConcern: WriteConcern(w: wMajority, wtimeout: 5000));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['average'], 92.33333333333333);
        expect(elements.first['grade'], 'A');
        expect(elements.last['average'], 75.66666666666667);
        expect(elements.last['grade'], 'C');
      });

      test('Specify collation', () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertFrenchCafe(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateOne(
            where
              ..$eq('category', 'cafe')
              ..$eq('status', 'a'),
            UpdateExpression()..$set('status', 'Updated'),
            collation: CollationOptions('fr', strength: 1));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 1);
        expect(res.nModified, 1);

        var elements = await collection
            .find(filter: where..$eq('status', 'Updated'))
            .toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 1);
        expect(elements.first['status'], 'Updated');
        expect(elements.first['category'], 'café');
      });
      test('Array filters - Update elements Match arrayFilters Criteria',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [95, 92, 90]
          },
          {
            '_id': 2,
            'grades': [98, 100, 102]
          },
          {
            '_id': 3,
            'grades': [95, 110, 100]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(where..$gte('grades', 100),
            UpdateExpression()..$set(r'grades.$[element]', 100),
            arrayFilters: [
              {
                'element': {r'$gte': 100}
              }
            ]);

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['grades'].first, 95);
        expect(elements.last['grades'].last, 100);
      });
      test('Array filters - Update Specific Elements of an Array of Documents',
          () async {
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var (ret, _, _, _) = await collection.insertMany([
          {
            '_id': 1,
            'grades': [
              {'grade': 80, 'mean': 75, 'std': 6},
              {'grade': 85, 'mean': 90, 'std': 4},
              {'grade': 85, 'mean': 85, 'std': 6}
            ]
          },
          {
            '_id': 2,
            'grades': [
              {'grade': 90, 'mean': 75, 'std': 6},
              {'grade': 87, 'mean': 90, 'std': 3},
              {'grade': 85, 'mean': 85, 'std': 4}
            ]
          }
        ]);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        var (res, _) = await collection.updateMany(
            where, UpdateExpression()..$set(r'grades.$[element].mean', 100),
            arrayFilters: [
              {
                'element.grade': {r'$gte': 85}
              }
            ]);

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 2);
        expect(res.nModified, 2);

        var elements = await collection.find().toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 2);
        expect(elements.first['grades'].last['mean'], 100);
        expect(elements.last['grades'].first['mean'], 100);
      });
      test('Specify hint', () async {
        if (!running4_2orGreater) {
          return;
        }
        var collectionName = getRandomCollectionName();
        var collection = db.collection(collectionName);

        var ret = await insertMembers(collection);
        expect(ret.ok, 1.0);
        expect(ret.isSuccess, isTrue);

        await collection.createIndex(keys: {'status': 1});
        await collection.createIndex(key: 'points');

        var (res, _) = await collection.updateMany(
            where
              ..$lte('points', 20)
              ..$eq('status', 'P'),
            UpdateExpression()..$set('misc1', 'Need to activate'),
            hint: HintUnion(<String, Object>{'status': 1}));

        expect(res, isNotNull);
        expect(res.ok, 1.0);
        expect(res.nMatched, 3);
        expect(res.nModified, 3);

        var elements =
            await collection.find(filter: where..$eq('status', 'P')).toList();

        expect(elements, isNotEmpty);
        expect(elements.length, 3);
        expect(elements.first['misc1'], 'Need to activate');
        expect(elements.last['misc1'], 'Need to activate');
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
