import 'package:mongo_db_driver/src/mongo_client.dart';

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

  var collectionName = 'delete-one';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany([
    {'_id': 3, 'name': 'John', 'age': 32},
    {'_id': 4, 'name': 'Mira', 'age': 27},
    {'_id': 7, 'name': 'Luis', 'age': 42},
  ]);
  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var (res, _) = await collection.deleteOne(<String, Object>{});

  print('Removed documents: ${res.nRemoved}'); // 1

  var findResult = await collection.find().toList();

  print('First record name: ${findResult.first['name']}'); // 'Mira';

  await cleanupDatabase();
}
