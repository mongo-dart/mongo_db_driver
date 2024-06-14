import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

const dbName = 'mongo-dart-example';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

void main() async {
  var running4_2orGreater = false;
  var client = MongoClient(defaultUri);
  await client.connect();
  var db = client.db();
  var serverFcv = db.server.serverCapabilities.fcv ?? '0.0';
  if (serverFcv.compareTo('4.2') != -1) {
    running4_2orGreater = true;
  }

  Future cleanupDatabase() async {
    await client.close();
  }

  if (!running4_2orGreater) {
    print('Not supported in this release');
    return;
  }

  var collectionName = 'find-modify-aggregation-pipeline';
  await db.drop(collectionName);
  var collection = db.collection(collectionName);

  var (ret, _, _, _) = await collection.insertMany([
    {
      '_id': 1,
      'grades': [
        {'grade': 80, 'mean': 75, 'std': 6},
        {'grade': 85, 'mean': 90, 'std': 4},
        {'grade': 85, 'mean': 85, 'std': 6}
      ],
    },
    {
      '_id': 2,
      'grades': [
        {'grade': 90, 'mean': 75, 'std': 6},
        {'grade': 87, 'mean': 90, 'std': 3},
        {'grade': 85, 'mean': 85, 'std': 4}
      ]
    }
  ]);
  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var (res, _) = await collection.findOneAndUpdate(
    where..$eq('_id', 1),
    (AggregationPipelineBuilder()
          ..addStage($addFields.raw({
            r'total': {r'$sum': r'$grades.grade'},
          })))
        .build(),
    returnNew: true,
  );
  print('Updated document: ${res.lastErrorObject?.updatedExisting}'); // true

  print('Modified element new total: '
      '${res.value?['total']}'); // 250;

  await cleanupDatabase();
}
