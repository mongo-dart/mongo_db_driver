import 'package:mongo_db_driver/mongo_db_driver.dart';

const dbName = 'mongo-dart-example';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

void main() async {
  var client = MongoClient(defaultUri);
  await client.connect();
  var db = client.db();
  Future cleanupDatabase() async {
    await client.close();
  }

  var collectionName = 'update-many-aggregate';
  await db.drop(collectionName);
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
  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var (res, _) = await collection.updateMany(
      null,
      (AggregationPipelineBuilder()
            ..addStage($set.raw({
              'status': 'Modified',
              'comments': [r'$misc1', r'$misc2']
            }))
            ..addStage($unset(['misc1', 'misc2'])))
          .build(),
      writeConcern: WriteConcern(w: W('majority'), wtimeout: 5000));

  print('Modified documents: ${res.nModified}'); // 2

  var findResult = await collection.find().toList();

  print('Last record status: ${findResult.last['status']}'); // 'Modified';

  await cleanupDatabase();
}
