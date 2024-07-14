import 'package:bson/bson.dart';
import 'package:mongo_db_driver/src/database/base/mongo_collection.dart';
import 'package:mongo_db_driver/src/client/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/mongo_dart-test');
  await client.connect();
  print('client connected');
  final db = client.db();
  ObjectId? id;
  MongoCollection coll;
  coll = db.collection('simple_data');
  await coll.deleteMany();
  print('Packing data to insert into collection by Bson...');
  for (var n = 0; n < 1000; n++) {
    await coll.insertOne({'my_field': n, 'str_field': 'str_$n'});
  }
  print('Done. Now sending it to MongoDb...');
  // ************************
  await coll
      .find(
          filter: where
            ..$gt('my_field', 995)
            ..sortBy('my_field'))
      .forEach((v) => print(v));
  // ************************
  var val = await coll.findOne(filter: where..$eq('my_field', 17));
  print('Filtered by my_field=17 $val');
  // ************************
  id = val?['_id'] as ObjectId?;
  if (id == null) {
    print('Id not detected');
    await client.close();
    return;
  }
  val = await coll.findOne(
      filter: where
        ..$eq('my_field', 17)
        ..fields.includeField('str_field'));
  print("findOne with fields clause 'str_field' $val");
  // ************************
  val = await coll.findOne(filter: where..id(id));
  print('Filtered by _id=$id: $val');
  // ************************
  print('Removing doc with _id=$id');
  await coll.deleteOne(where..id(id));
  val = await coll.findOne(filter: where..id(id));
  print('Filtered by _id=$id: $val. There more no such a doc');
  // ************************
  await coll
      .find(
          filter: (where
                ..$gt('my_field', 995)
                ..$or
                ..$lt('my_field', 10)
                ..$and
                ..$regex('str_field', '99'))
              .rawFilter)
      .forEach((v) => print(v));
  print(
      "Filtered by (my_field gt 995 or my_field lt 10) and str_field like '99' ");
  // ************************
  await coll
      .find(
          filter: where
            ..inRange('my_field', 700, 703, minInclude: false)
            ..sortBy('my_field'))
      .forEach((v) => print(v));
  print('Filtered by my_field gt 700, lte 703');
  // ************************
  await coll
      .find(
          filter: where
            ..inRange('my_field', 700, 703, minInclude: false)
            ..sortBy('my_field'))
      .forEach((v) => print(v));
  print("Filtered by str_field match '^str_(5|7|8)17\$'");
  // ************************
  await coll
      .find(
          filter: where
            ..$regex('str_field', 'str_(5|7|8)17\$')
            ..sortBy({'str_field': -1})
            ..sortBy('my_field'))
      .forEach((v) => print(v));
  // ************************
  // TODO Check
  var explanation = await coll.findOne(
      filter: where
        ..$regex('str_field', 'str_(5|7|8)17\$')
        ..sortBy({'str_field': -1})
        ..sortBy('my_field')
        ..explain());
  print('Query explained: $explanation');
  // ************************
  print('Now where clause with jscript code: '
      "where..\$where('this.my_field % 100 == 35')");
  await coll
      .find(filter: where..$where('this.my_field % 100 == 35'))
      .forEach((v) => print(v));
  // ************************
  print('Now where clause with jscript code: '
      "where..\$where('function() {return this.my_field % 100 == 10}')");
  await coll
      .find(
          filter: where
            ..$where('function() {return this.my_field % 100 == 10}'))
      .forEach((v) => print(v));
  // ************************
  var count = await coll.count(selector: where..$gt('my_field', 995));
  print('Count of records with my_field > 995: $count'); // 4
  // ************************
  var databases = await client.listDatabases();
  print('List of databases: $databases');
  // ************************
  var collections = await db.getCollectionNames();
  print('List of collections : $collections');
  // ************************
  var collectionInfos = await db.getCollectionInfos();
  print('List of collection\'s infos: $collectionInfos');
  await client.close();
}
