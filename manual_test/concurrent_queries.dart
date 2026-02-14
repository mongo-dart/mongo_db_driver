import 'package:mongo_db_driver/mongo_db_driver.dart';

const concurrentQueries = 5;
const dbName = 'mongo-dart-example';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';
const defaultUri2 =
    'mongodb+srv://-user-:-pwd-@-address-.mongodb.net/'
    '?appName=Flex&authMechanism=SCRAM-SHA-1&retryWrites=true&w=majority';

void main() async {
  var client = MongoClient(
    defaultUri2,
    mongoClientOptions: MongoClientOptions()..tls = true,
  );
  await client.connect();
  final db = client.db();
  final collection = db.collection('test_collection');
  var result = await Future.wait([
    for (var i = 0; i < concurrentQueries; i++) collection.find().toList(),
  ]);
  print("-");
  print(result);
  await client.close();
}
