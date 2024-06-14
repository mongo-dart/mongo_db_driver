import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/mongo_dart-test');
  await client.connect();
  var db = client.db();
  ///// Simple update
  var coll = db.collection('collection-for-save');
  await coll.deleteMany();
  var toInsert = <Map<String, dynamic>>[
    {'name': 'a', 'value': 10},
    {'name': 'b', 'value': 20},
    {'name': 'c', 'value': 30},
    {'name': 'd', 'value': 40}
  ];
  await coll.insertMany(toInsert);
  var v1 = await coll.findOne(filter: {'name': 'c'});
  if (v1 == null) {
    print('Record not found');
    await client.close();
    return;
  }
  print('Record c: $v1');
  v1['value'] = 31;
  await coll.replaceOne({'name': 'c'}, v1);
  var v2 = await coll.findOne(filter: {'name': 'c'});
  print('Record c after update: $v2');

  /////// Field level update
  coll = db.collection('collection-for-save');
  await coll.deleteMany();
  toInsert = <Map<String, dynamic>>[
    {'name': 'a', 'value': 10},
    {'name': 'b', 'value': 20},
    {'name': 'c', 'value': 30},
    {'name': 'd', 'value': 40}
  ];
  await coll.insertMany(toInsert);
  v1 = await coll.findOne(filter: {'name': 'c'});
  print('Record c: $v1');
  await coll.updateOne(where..$eq('name', 'c'), modify..$set('value', 31));
  v2 = await coll.findOne(filter: {'name': 'c'});
  print('Record c after field level update: $v2');

  //// Field level increment
  coll = db.collection('collection-for-save');
  await coll.deleteMany();
  toInsert = <Map<String, dynamic>>[
    {'name': 'a', 'value': 10},
    {'name': 'b', 'value': 20},
    {'name': 'c', 'value': 30},
    {'name': 'd', 'value': 40}
  ];
  await coll.insertMany(toInsert);
  v1 = await coll.findOne(filter: {'name': 'c'});
  print('Record c: $v1');
  await coll.updateOne(where..$eq('name', 'c'), modify..$inc('value', 2));
  v2 = await coll.findOne(filter: {'name': 'c'});
  print('Record c after field level increment by two: $v2');
  await client.close();
}
