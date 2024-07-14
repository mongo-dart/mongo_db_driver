import 'package:decimal/decimal.dart';
import 'package:mongo_db_driver/src/client/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/testdb');
  await client.connect();
  final db = client.db();
  var collection = db.collection('orders');
  await collection.drop();
  await collection.insertMany([
    {'status': 'A', 'cust_id': 3, 'amount': Decimal.fromInt(128)},
    {'status': 'B', 'cust_id': 2, 'amount': Decimal.fromInt(100)},
    {'status': 'A', 'cust_id': 1, 'amount': Decimal.fromInt(80)},
    {'status': 'A', 'cust_id': 3, 'amount': Decimal.fromInt(72)},
  ]);
  final pipeline = pipelineBuilder
    ..addStage($match((where..$eq('status', 'A'))))
    ..addStage(
        $group(id: Field('cust_id'), fields: {'total': $sum(Field('amount'))}));

  final result = await collection.aggregateToStream(pipeline).toList();
  result.forEach(print);
  // {_id: 3, total: 200}
  // {_id: 1, total: 80}
}
