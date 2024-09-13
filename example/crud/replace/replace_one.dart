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

  var collectionName = 'replace-one';
  await db.drop(collectionName);
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
  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var (res, _) = await collection.replaceOne(
      where..$eq('member', 'abc123'),
      {
        '_id': 1,
        'member': 'abc123',
        'status': 'A',
        'points': 1,
        'misc1': 'note to self: confirm status',
        'misc2': 'Need to activate'
      },
      writeConcern: WriteConcern(w: W('majority'), wtimeout: 5000));

  print('Modified documents: ${res.nModified}'); // 1

  var findResult =
      await collection.find(filter: where..$eq('member', 'abc123')).toList();

  print('Modified element status: '
      '"${findResult.first['status']}"'); // 'A';

  await cleanupDatabase();
}
