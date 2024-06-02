import 'package:logging/logging.dart'
    show Level, LogRecord, Logger, hierarchicalLoggingEnabled;
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/mongo_dart_old.dart';

const dbName = 'mongo-dart-example';
const dbAddress = 'localhost';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

void main() async {
  hierarchicalLoggingEnabled = true;
  Logger('Mongoconnection example').level = Level.INFO;

  void listener(LogRecord r) {
    var name = r.loggerName;
    print('${r.time}: $name: ${r.message}');
  }

  Logger.root.onRecord.listen(listener);

  var client = MongoClient(defaultUri,
      mongoClientOptions: MongoClientOptions()
        ..serverApi = ServerApi(ServerApiVersion.v1));
  await client.connect();
  var db = client.db();

  Future cleanupDatabase() async {
    await client.close();
  }

  var collectionName = 'insert-one';
  var collection2Name = 'update-one';

  await db.drop(collectionName);
  await db.drop(collection2Name);

  var collection = db.collection(collectionName);
  var collection2 = db.collection(collection2Name);

  // *** Simple case ***
  var (ret, _, _, _) = await collection.insertOne(<String, dynamic>{
    '_id': 1,
    'name': 'Tom',
    'state': 'active',
    'rating': 100,
    'score': 5
  });

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  var res = await collection.findOne();

  print('Fetched: "${res?['name']}"');
  // Tom

// *** In Session ***
  var session = client.startSession();
  (ret, _, _, _) = await collection.insertOne(<String, dynamic>{
    '_id': 2,
    'name': 'Ezra',
    'state': 'active',
    'rating': 90,
    'score': 6
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }
  await session.endSession();

  res = await collection.findOne(filter: where..$eq('rating', 90));
  print('Fetched: "${res?['name']}"');
  // Ezra

// *** In Transaction committed ***
  session = client.startSession();
  session.startTransaction();
  var (ret2, _) = await collection.updateOne(
      where..$eq('rating', 90), modify..$inc('score', 1),
      session: session);
  if (!ret2.isSuccess) {
    print('Error detected in record update');
  }

  (ret, _, _, _) = await collection2.insertOne(<String, dynamic>{
    '_id': 3,
    'name': 'Nathan',
    'state': 'active',
    'rating': 98,
    'score': 4
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }
  var commitRes = await session.commitTransaction();
  if (commitRes?[keyOk] == 0.0) {
    print('${commitRes?[keyErrmsg]}');
  }
  await session.endSession();

  res = await collection2.findOne(filter: where..sortBy('score'));
  print('Fetched: "${res?['name']}"');
  // Nathan

  res = await collection.findOne(filter: where..$eq('name', 'Ezra'));
  print('Fetched: "${res?['score']}"');
  // 7

// *** In Transaction aborted ***
  session = client.startSession();
  session.startTransaction();
  (ret, _, _, _) = await collection.insertOne(<String, dynamic>{
    '_id': 4,
    'name': 'Anne',
    'state': 'inactive',
    'rating': 120,
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }

  (ret, _, _, _) = await collection2.insertOne(<String, dynamic>{
    '_id': 4,
    'name': 'Anne',
    'state': 'inactive',
    'rating': 120,
  }, session: session);

  if (!ret.isSuccess) {
    print('Error detected in record insertion');
  }
  await session.abortTransaction();
  await session.endSession();

  res = await collection.findOne(filter: where..sortBy('name'));
  print('Fetched: "${res?['name']}"');
  // Ezra

  var retList = await collection2.find().toList();
  print('Fetched: "${retList.length}"');
  // 1

  await cleanupDatabase();
}

// Expected:
// Fetched: "Tom"
// Fetched: "Ezra"
// Fetched: "Nathan"
// Fetched: "7"
// Fetched: "Ezra"
// Fetched: "1"
