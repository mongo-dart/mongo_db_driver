import 'dart:async';

import 'package:mongo_db_driver/src/command/command_exp.dart';
import 'package:mongo_db_driver/src/database/database_exp.dart';
import 'package:mongo_db_driver/src/client/mongo_client.dart';
import 'package:mongo_db_driver/src/unions/query_union.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/test');
  await client.connect();
  var db = client.db();
  await db.drop('log');
  await db.createCollection('log',
      createCollectionOptions:
          CreateCollectionOptions(capped: true, size: 1024));
  var i = 0;
  await db.collection('log').insertOne({'index': i});
  Timer.periodic(Duration(seconds: 1), (Timer t) async {
    i++;
    print('Insert $i');
    await db.collection('log').insertOne({'index': i});
    if (i == 10) {
      print('Stop inserting');
      t.cancel();
    }
  });

  var oplog = MongoCollection(db, 'log');
  var cursor = Cursor(
      FindOperation(oplog, QueryUnion({}),
          findOptions: FindOptions(
              tailable: true, noCursorTimeout: true, awaitData: false)),
      db.server);
  while (true) {
    var doc = await cursor.nextObject();
    if (doc == null) {
      print('.');
      await Future.delayed(Duration(milliseconds: 200), () => null);
    } else {
      print('Fetched: $doc');
      if (doc['index'] == 10) {
        break;
      }
    }
  }
  print('Test ended!');
  await client.close();
}
