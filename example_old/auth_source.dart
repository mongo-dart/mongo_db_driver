import 'package:mongo_db_driver/src/mongo_client.dart';

const dbName = 'mongodb-auth';
const dbAddress = '127.0.0.1';
const mongoDbUri = 'mongodb://test:test@$dbAddress:27031/$dbName';

void main() async {
  var client = MongoClient(mongoDbUri);
  await client.connect();
  var db = client.db();
  var collection = db.collection('test');
  print(await collection.find().toList());
  await client.close();
}
