import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

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

  var collectionName = 'find-modify-array-filters';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany([
    {
      '_id': 1,
      'grades': [95, 92, 90]
    },
    {
      '_id': 2,
      'grades': [98, 100, 102]
    },
    {
      '_id': 3,
      'grades': [95, 110, 100]
    }
  ]);
  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var (res, _) = await collection.findOneAndUpdate(where..$gte('grades', 100),
      UpdateExpression()..$set(r'grades.$[element]', 100),
      returnNew: true,
      arrayFilters: [
        {
          'element': {r'$gte': 100}
        }
      ]);
  print('Updated document: ${res.lastErrorObject?.updatedExisting}'); // true

  print('Modified element new grades: '
      '${res.value?['grades']}'); // [98, 100, 100];

  await cleanupDatabase();
}
