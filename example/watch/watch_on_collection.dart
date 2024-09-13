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

  var collection = db.collection('watch-collection');
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
  /// ***IMPORTANT ***
  /// As the change stream return a "fullDocument" and all
  /// checks are made on this document, all field names must be prefixed
  /// with "fullDocument" (see below: 'fullDocument.custId')
  ///
  /// Inside the Match stage there is the query operator "oneFrom" that
  /// corresponds to "$in"
  ///
  /// *** Note***
  /// If you use a QueryExpression the Match stage requires a Map, so
  /// you have to extract the map with ".map['\$query']"
  var pipeline = AggregationPipelineBuilder()
    ..addStage($match(where..$in('fullDocument.custId', [1, 2])));

  /// If you look for updates is better to set "fullDocument" to "updateLookup"
  /// otherwise the returned document will contain only the changed fields
  ///
  /// *** Note ***
  /// As the pipeline control is made on the document processed,
  /// If the document does not contains the field to be verified,
  /// the event vill not be fired.
  /// In our case, if we do not specify 'updateLookup' the returned document
  /// will not contain the 'custId' field and the match
  /// {'custId': {r'$in': [1, 2]}} will not be performed (no event returned)
  var stream = collection.watch(pipeline,
      changeStreamOptions: ChangeStreamOptions(fullDocument: 'updateLookup'));

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

  var waitingCount = 0;
  await Future.doWhile(() async {
    if (pleaseClose) {
      print('Change detected, closing stream and db.');

      /// This is the correct way to cancel the watch subscription
      await controller.cancel();
      await client.close();
      return false;
    }
    print('Waiting for change to be detected...');
    await Future.delayed(Duration(seconds: 2));
    waitingCount++;
    if (waitingCount > 7) {
      throw StateError('Something went wrong :-(');
    }

    return true;
  });
}
