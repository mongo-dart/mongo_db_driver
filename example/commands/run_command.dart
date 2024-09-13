import 'dart:io';

import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';

const dbName = 'mongo-dart-example';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

void main() async {
  var client = MongoClient(defaultUri);
  await client.connect();
  var db = client.db();
  await db.dropDatabase();

  Future cleanupDatabase() async {
    await client.close();
  }

  var ret = await db.runCommand({'ping': 1});
  print(ret); // {ok: 1.0};

  ret = await CommandOperation(db, {'ping': 1}, <String, Object>{}).process();
  print(ret); // {ok: 1.0};

  ret = await PingCommand(client).process();
  print(ret); // {ok: 1.0};

  ret = await db.pingCommand();
  print(ret); // {ok: 1.0};

  var result = await db.collection(r'$cmd').findOne(filter: {'ping': 1});
  print(result); // {ok: 1.0};

  try {
    await db.collection(r'$cmd').find(filter: {'ping': 1}).toList();
    print('***** -> Unexpected behaviour');
  } catch (error) {
    print(error);
    // "MongoDart Error: Invalid collection name specified 'mongo-dart-example.$cmd'";
  }

  await cleanupDatabase();

  exit(0);
}
