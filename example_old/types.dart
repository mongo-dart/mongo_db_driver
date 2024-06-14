import 'package:bson/bson.dart';
import 'package:mongo_db_driver/src/mongo_client.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/mongo_dart-blog');
  await client.connect();
  var db = client.db();
  var collection = db.collection('test-types');
  await collection.deleteMany();
  await collection.insertOne({
    'array': [1, 2, 3],
    'string': 'hello',
    'hash': {'a': 1, 'b': 2},
    'date': DateTime.now(), // Stores only milisecond resolution
    'oid': ObjectId(),
    'binary': BsonBinary.from([0x23, 0x24, 0x25]),
    'int': 42,
    'float': 33.3333,
    'regexp': RegExp('.?dim'),
    'boolean': true,
    'where': JsCode('this.x == 3'),
    'null': null
  });
  var v = await collection.findOne();
  print(v);
  await client.close();
}
