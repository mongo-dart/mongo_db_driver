// ignore_for_file: unused_local_variable

import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:test/test.dart';

import '../utils/test_database.dart';

const dbName = 'mongo-dart-basic-session';
const dbServerName = 'session';
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

  group('Basic Session Test', () {
    tearDownAll(() async {
      await cleanupDatabase();
    });

    group('Session', () {
      if (!testInfo.serverfound || testInfo.isStandalone) {
        return;
      }
      test('Implicit Session', () async {
        var db = client.db();
        var collection = db.collection('session-test');
        await collection.drop();

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
      });
      test('Explicit Session', () async {
        var db = client.db();
        var collection = db.collection('session-test-2');
        await collection.drop();

        var session = client.startSession();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        await session.endSession();
      });
      test('Explicit Session with No end session', () async {
        var db = client.db();
        var collection = db.collection('session-test-3');
        await collection.drop();

        var session = client.startSession();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        //await session.endSession();
      });
      test('Two Sessions in sequence', () async {
        var db = client.db();
        var collection = db.collection('session-test-4');
        await collection.drop();
        var session = client.startSession();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        await session.endSession();
        session = client.startSession();
        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Maria');
        await session.endSession();
        var findResult = await collection.find().toList();
        expect(findResult.length, 2);
        expect(findResult.first['Name'], 'Jack');
      });
      test('Two Sessions in sequence, first not ended', () async {
        var db = client.db();
        var collection = db.collection('session-test-4');
        await collection.drop();
        var session = client.startSession();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        //await session.endSession();
        session = client.startSession();
        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);

        result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Maria');
        await session.endSession();
        var findResult = await collection.find().toList();
        expect(findResult.length, 2);
        expect(findResult.first['Name'], 'Jack');
      });

      test('Two Sessions in parallel', () async {
        var db = client.db();
        var collection = db.collection('session-test-4');
        await collection.drop();
        var session = client.startSession();
        var session2 = client.startSession();

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);
        var result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Jack');
        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session2,
            insertOneOptions:
                InsertOneOptions(writeConcern: WriteConcern.majority));
        expect(writeResult.isSuccess, isTrue);
        result = await collection.findOne(filter: where..id(id));
        expect(result?['Name'], 'Maria');
        var findResult = await collection.find().toList();
        expect(findResult.length, 2);
        expect(findResult.last['Name'], 'Maria');

        await session.endSession();
        await session2.endSession();

        findResult = await collection.find().toList();
        expect(findResult.length, 2);
        expect(findResult.first['Name'], 'Jack');
      });
    });

    group('Transaction', () {
      if (!testInfo.serverfound || testInfo.isStandalone) {
        return;
      }

      test('Simple transaction', () async {
        var db = client.db();
        var collection = db.collection('trx-test');
        await collection.drop();

        var session = client.startSession();
        session.startTransaction();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        // Test Read with session
        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        // Test Read without session
        result = await collection.findOne(filter: where..id(id));
        expect(result, isNull);

        await session.commitTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);
        await session.endSession();

        // Test Read after commit
        result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');
      });
      test('Simple transaction aborted', () async {
        var db = client.db();
        var collection = db.collection('trx-test-2');
        await collection.drop();

        var session = client.startSession();
        session.startTransaction();
        expect(session.inTransaction, isTrue);

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);
        expect(session.inTransaction, isTrue);

        // Test Read with session
        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        // Test Read without session
        result = await collection.findOne(filter: where..id(id));
        expect(result, isNull);

        expect(session.inTransaction, isTrue);
        await session.abortTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.aborted);
        await session.endSession();

        // Test Read after commit
        result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result, isNull);
      });
      test('Simple transaction implicitly aborted', () async {
        var db = client.db();
        var collection = db.collection('trx-test');
        await collection.drop();

        var session = client.startSession();
        session.startTransaction();
        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        // Test Read with session
        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        // Test Read without session
        result = await collection.findOne(filter: where..id(id));
        expect(result, isNull);

        await session.endSession();

        // Test Read after commit
        result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result, isNull);
      });
      test('Simple empty transaction', () async {
        var db = client.db();
        var collection = db.collection('trx-test-4');
        await collection.drop();

        var session = client.startSession();
        expect(session.transaction.state, TransactionState.none);

        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        await session.commitTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committedEmpty);
        await session.endSession();

        // Test Read after commit
        var result = await collection.findOne();
        expect(result, null);
      });

      test('Two transactions in sequence', () async {
        var db = client.db();
        var collection = db.collection('trx-test-5');
        await collection.drop();

        var session = client.startSession();
        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.none);

        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        await session.commitTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);

        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Maria');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        await session.commitTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);

        await session.endSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);

        var findResult = await collection.find().toList();

        expect(findResult.length, 2);
        expect(findResult.first['Name'], 'Jack');
      });
      test('Two transactions in sequence - first aborted', () async {
        var db = client.db();
        var collection = db.collection('trx-test-6');
        await collection.drop();

        var session = client.startSession();
        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.none);

        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        await session.abortTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.aborted);

        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Maria');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        await session.commitTransaction();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);

        await session.endSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);

        var findResult = await collection.find().toList();

        expect(findResult.length, 1);
        expect(findResult.first['Name'], 'Maria');
      });
      test('Two transactions in parallel', () async {
        var db = client.db();
        var collection = db.collection('trx-test-7');
        await collection.drop();
        var collection2 = db.collection('trx-test-7b');
        await collection2.drop();

        var session = client.startSession();
        var session2 = client.startSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.none);

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.none);

        //print('Start Session');
        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        //print('insert on session');

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        //print('find on session');
        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        //print('start session 2');

        session2.startTransaction();

        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.starting);

        //print('insert one session 2');
        (writeResult, _, _, id) = await collection2.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session2);
        expect(writeResult.isSuccess, isTrue);

        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.inProgress);

        //print('find one session 2');
        result =
            await collection2.findOne(filter: where..id(id), session: session2);
        expect(result?['Name'], 'Maria');

        // -------------------
        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        //print('commit session');
        await session.commitTransaction();
        // -------------------

        // -------------------
        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.inProgress);

        //print('commit session2');
        var ret = await session2.commitTransaction();

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isTrue);
        expect(session2.transaction.state, TransactionState.committed);
        // -------------------

        // -------------------
        await session.endSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);
        // -------------------

        // -------------------
        await session2.endSession();

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isTrue);
        expect(session2.transaction.state, TransactionState.committed);
        // -------------------

        var findResult = await collection2.find().toList();

        expect(findResult.length, 1);
        expect(findResult.first['Name'], 'Maria');
      });
      test('Two transactions in parallel on same resources', () async {
        Future<MongoDocument> commitWithRetry(session,
            {int stopOnTransient = 0}) async {
          MongoDocument ret = await session.commitTransaction();
          if (ret[keyOk] == 0.0) {
            if (ret.containsKey('errorLabels') &&
                ret['errorLabels']
                    ?.contains('UnknownTransactionCommitResult')) {
              print(
                  'UnknownTransactionCommitResult, retrying commit operation ...');
              await commitWithRetry(session);
            } else if (stopOnTransient < 5 &&
                ret.containsKey('errorLabels') &&
                ret['errorLabels']?.contains('TransientTransactionError')) {
              print('TransientTransactionError, retrying commit operation ...');
              await commitWithRetry(session,
                  stopOnTransient: stopOnTransient + 1);
            } else {
              print('Error during commit ...');
              print(ret);
              throw ret[keyErrmsg];
            }
          }
          print('Transaction committed.');
          return ret;
        }

        var db = client.db();
        var collection = db.collection('trx-test-7');
        await collection.drop();

        var session = client.startSession();
        var session2 = client.startSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.none);

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.none);

        print('Start Session');
        session.startTransaction();

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.starting);

        print('insert on session');

        var (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Jack', 'DateOfBirth': DateTime(1987, 5, 12), 'Score': 98},
            session: session);
        expect(writeResult.isSuccess, isTrue);

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        print('find on session');
        var result =
            await collection.findOne(filter: where..id(id), session: session);
        expect(result?['Name'], 'Jack');

        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        print('start session 2');

        session2.startTransaction();

        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.starting);

        print('insert one session 2');
        (writeResult, _, _, id) = await collection.insertOne(
            {'Name': 'Maria', 'DateOfBirth': DateTime(1999, 8, 4), 'Score': 85},
            session: session2);
        expect(writeResult.isSuccess, isTrue);

        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.inProgress);

        print('find one session 2');
        result =
            await collection.findOne(filter: where..id(id), session: session2);
        expect(result?['Name'], 'Maria');

        // -------------------
        expect(session.inTransaction, isTrue);
        expect(session.isTransactionCommitted, isFalse);
        expect(session.transaction.state, TransactionState.inProgress);

        print('commit session');
        await session.commitTransaction();
        // -------------------

        // -------------------
        expect(session2.inTransaction, isTrue);
        expect(session2.isTransactionCommitted, isFalse);
        expect(session2.transaction.state, TransactionState.inProgress);

        print('commit session2');
        await commitWithRetry(session2);
        //var ret = await session2.commitTransaction();

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isTrue);
        expect(session2.transaction.state, TransactionState.committed);
        // -------------------

        // -------------------
        await session.endSession();

        expect(session.inTransaction, isFalse);
        expect(session.isTransactionCommitted, isTrue);
        expect(session.transaction.state, TransactionState.committed);
        // -------------------

        // -------------------
        await session2.endSession();

        expect(session2.inTransaction, isFalse);
        expect(session2.isTransactionCommitted, isTrue);
        expect(session2.transaction.state, TransactionState.committed);
        // -------------------

        var findResult = await collection.find().toList();

        expect(findResult.length, 2);
        expect(findResult.last['Name'], 'Maria');
      });
    });
  });
}
