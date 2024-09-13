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

  var collectionName = 'find-array';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany(<Map<String, dynamic>>[
    {
      '_id': 1,
      'admin': 'Tom',
      'state': 'active',
      'employers': [ObjectId.fromHexString('624f96b5210107050e87565a')]
    },
    {
      '_id': 2,
      'admin': 'William',
      'state': 'busy',
    },
    {
      '_id': 3,
      'admin': 'Liz',
      'state': 'on hold',
    },
    {
      '_id': 4,
      'admin': 'George',
      'state': 'active',
      'employers': [
        ObjectId.fromHexString('624f96b5210107050e875687'),
        ObjectId.fromHexString('624f96b5210107050e875692'),
        ObjectId.fromHexString('624f96b5210107050e875698')
      ]
    },
    {
      '_id': 5,
      'admin': 'Jim',
      'state': 'idle',
    },
    {
      '_id': 6,
      'admin': 'Laureen',
      'state': 'busy',
      'employers': [
        ObjectId.fromHexString('624f96b52101070512875687'),
        ObjectId.fromHexString('624f96b52101070512875692'),
        ObjectId.fromHexString('624f96b5210112050e875698')
      ]
    },
    {
      '_id': 7,
      'admin': 'John',
      'state': 'idle',
    }
  ]);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var res = await collection
      .find(
          filter: where
            ..$eq('employers',
                ObjectId.fromHexString('624f96b5210112050e875698')))
      .toList();
  print('Number of documents fetched: ${res.length}'); // 1
  print(
      'First document fetched: ${res.first['admin']} - ${res.first['state']}');
  // Laureen - busy

  await cleanupDatabase();
}
