@Timeout(Duration(minutes: 10))
library;

import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'dart:async';
import 'package:test/test.dart';
import 'package:uuid/uuid.dart';

import 'utils/insert_bio_db.dart';

part 'crud/insert/insert_one.dart';
part 'crud/insert/insert_many.dart';

part 'crud/insert/insert.dart';

part 'crud/find/find.dart';
part 'crud/find/find_simple.dart';
part 'crud/find/find_default.dart';

const dbName = 'test-mongo-dart-crud';
const dbAddress = '127.0.0.1';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

Uuid uuid = Uuid();
List<String> usedCollectionNames = [];

String getRandomCollectionName(List<String> collectionNames) {
  var name = 'c-${uuid.v4()}';
  collectionNames.add(name);
  return name;
}

Future<MongoDatabase> initializeDatabase(MongoClient client) async {
  await client.connect();
  return client.db();
}

Future cleanupDatabase(MongoClient client) async {
  await client.close();
}

void main() async {
  await insertTest();
  await findTest();
}
