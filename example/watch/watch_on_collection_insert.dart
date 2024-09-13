import 'package:mongo_db_driver/mongo_db_driver.dart';

/// Watch does not work on Standalone systems
/// Only Replica Set and Sharded Cluster
///
/// The actual implementation of watch only works for document event,
/// not for database or collection ones (like drop)
/// [Give a look to this also](https://docs.mongodb.com/manual/changeStreams/)
void main() async {
  var client = MongoClient('mongodb://127.0.0.1/testdb');
  await client.connect();
  var db = client.db();

  var collection = db.collection('watch-collection-insert');
  // clean data if the example is run more than once.
  await collection.drop();

  await collection.insertMany([
    {'custId': 1, 'name': 'Jeremy'},
    {'custId': 2, 'name': 'Al'},
    {'custId': 3, 'name': 'John'},
  ]);

  /// Only some stages can be used in the pipeline for a change stream:
  /// - $addFields
  /// - $match
  /// - $project
  /// - $replaceRoot
  /// - $replaceWith (Available starting in MongoDB 4.2)
  /// - $redact
  /// - $set (Available starting in MongoDB 4.2)
  /// - $unset (Available starting in MongoDB 4.2)
  ///
  /// If you look for updates is better to set "fullDocument" to "updateLookup"
  /// otherwise the returned document will contain only the changed fields
  ///
  /// *** Note ***
  /// In our case, if we do not specify 'updateLookup' the returned document
  /// will not contain the 'custId' field (for updates)
  var stream = collection.watch(<Map<String, Object>>[
    {
      r'$match': {'operationType': 'insert'}
    }
  ] /* ,
      changeStreamOptions: ChangeStreamOptions(fullDocument: 'updateLookup') */
      );

  var pleaseClose = false;

  /// As the stream does not end until it is closed, do not use .toList()
  /// or you will wait indefinitely
  var controller = stream.listen((changeEvent) {
    Map<String, dynamic> fullDocument =
        changeEvent.fullDocument ?? <String, dynamic>{};

    print('Detected change for "custId" '
        '${fullDocument['custId']}: "${fullDocument['name']}"');

    pleaseClose = true;
  });

  /// The event will be emitted only when the majority of the
  /// replicas has acknowledged the change.
  /// This is default behavior starting from 4.2, in 4.0 and earlier you have
  /// to set the writeConcern to 'majority' or the events will not be emitted
  await collection.updateOne(
      where..$eq('custId', 1), UpdateExpression()..$set('name', 'Harry'),
      writeConcern: WriteConcern.majority);

  await collection.insertOne({'custId': 4, 'name': 'Nathan'},
      insertOneOptions: InsertOneOptions(writeConcern: WriteConcern.majority));

  var waitingCount = 0;
  await Future.doWhile(() async {
    if (pleaseClose) {
      print('Insert detected, closing stream and db.');

      /// This is the correct way to cancel the watch subscription
      await controller.cancel();
      await client.close();
      return false;
    }
    print('Waiting for insert to be detected...');
    await Future.delayed(Duration(seconds: 2));
    waitingCount++;
    if (waitingCount > 7) {
      throw StateError('Something went wrong :-(');
    }

    return true;
  });
}
