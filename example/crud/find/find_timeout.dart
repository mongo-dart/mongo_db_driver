import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/find_operation/base/find_options.dart';
import 'package:mongo_db_driver/src/client/mongo_client.dart';

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

  var collectionName = 'find-timeout';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany(<Map<String, dynamic>>[
    {'_id': 1, 'name': 'Tom', 'state': 'active', 'rating': 100, 'score': 5},
    {'_id': 2, 'name': 'William', 'state': 'busy', 'rating': 80, 'score': 4},
    {'_id': 3, 'name': 'Liz', 'state': 'on hold', 'rating': 70, 'score': 8},
    {'_id': 4, 'name': 'George', 'state': 'active', 'rating': 95, 'score': 8},
    {'_id': 5, 'name': 'Jim', 'state': 'idle', 'rating': 40, 'score': 3},
    {'_id': 6, 'name': 'Laureen', 'state': 'busy', 'rating': 87, 'score': 8},
    {'_id': 7, 'name': 'John', 'state': 'idle', 'rating': 72, 'score': 7}
  ]);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var res = await collection.find(filter: {
    'rating': {r'$gt': 60}
  }, sort: {
    'state': 1,
    'name': 1
  }, findOptions: FindOptions(maxTimeMS: 5000)).toList();

  print('First document fetched: ${res.first['name']} - ${res.first['state']}');
  // George - active

  await cleanupDatabase();
}
