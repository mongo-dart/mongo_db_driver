import 'package:mongo_db_driver/src/client/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:uuid/uuid.dart';

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

  var collectionName = 'find-one';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany(<Map<String, dynamic>>[
    {
      '_id': 1,
      'name': 'Tom',
      'state': 'active',
      'rating': 100,
      'score': 5,
      'userId': UuidValue.fromString('025456da-9e39-4e7c-b1f7-0f5a5e1cb212')
    },
    {
      '_id': 2,
      'name': 'William',
      'state': 'busy',
      'rating': 80,
      'score': 4,
      'userId': Uuid().v4obj()
    },
    {
      '_id': 3,
      'name': 'Liz',
      'state': 'on hold',
      'rating': 70,
      'score': 8,
      'userId': Uuid().v4obj()
    },
    {
      '_id': 4,
      'name': 'George',
      'state': 'active',
      'rating': 95,
      'score': 8,
      'userId': Uuid().v4obj()
    },
    {
      '_id': 5,
      'name': 'Jim',
      'state': 'idle',
      'rating': 40,
      'score': 3,
      'userId': Uuid().v4obj()
    },
    {
      '_id': 6,
      'name': 'Laureen',
      'state': 'busy',
      'rating': 87,
      'score': 8,
      'userId': Uuid().v4obj()
    },
    {
      '_id': 7,
      'name': 'John',
      'state': 'idle',
      'rating': 72,
      'score': 7,
      'userId': Uuid().v4obj()
    }
  ]);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var res = await collection.findOne(
      filter: where
        ..$eq('name', 'Tom')
        ..$gt('rating', 10));

  if (res == null) {
    print('No document found');
  } else {
    print('Document fetched: '
        '${res['name']} - ${res['state']} - ${res['userId']}');
  } // Tom - active - 025456da-9e39-4e7c-b1f7-0f5a5e1cb212

  await cleanupDatabase();
}
