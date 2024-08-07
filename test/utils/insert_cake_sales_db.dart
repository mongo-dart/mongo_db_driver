import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

Future<(BulkWriteResult, MongoDocument, List<MongoDocument>, List<dynamic>)>
    insertCakeSales(MongoCollection collection) async {
  return collection.insertMany([
    {
      '_id': 0,
      'type': 'chocolate',
      'orderDate': DateTime.parse('2020-05-18T14:10:30Z'),
      'state': "CA",
      'price': 13,
      'quantity': 120
    },
    {
      '_id': 1,
      'type': 'chocolate',
      'orderDate': DateTime.parse('2021-03-20T11:30:05Z'),
      'state': "WA",
      'price': 14,
      'quantity': 140
    },
    {
      '_id': 2,
      'type': 'vanilla',
      'orderDate': DateTime.parse('2021-01-11T06:31:15Z'),
      'state': "CA",
      'price': 12,
      'quantity': 145
    },
    {
      '_id': 3,
      'type': 'vanilla',
      'orderDate': DateTime.parse('2020-02-08T13:13:23Z'),
      'state': "WA",
      'price': 13,
      'quantity': 104
    },
    {
      '_id': 4,
      'type': 'strawberry',
      'orderDate': DateTime.parse('2019-05-18T16:09:01Z'),
      'state': "CA",
      'price': 41,
      'quantity': 162
    },
    {
      '_id': 5,
      'type': 'strawberry',
      'orderDate': DateTime.parse('2019-01-08T06:12:03Z'),
      'state': "WA",
      'price': 43,
      'quantity': 134
    }
  ]);
}
