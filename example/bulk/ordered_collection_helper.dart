import 'package:mongo_db_driver/mongo_db_driver.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/testdb');
  await client.connect();
  final db = client.db();

  var collection = db.collection('orderedBulkHelper');
  // clean data if the example is run more than once.
  await collection.drop();

  /// Bulk write returns an instance of `BulkWriteResult` class
  /// that is a convenient way of reading the result data.
  /// If you prefer the server response, a serverResponses list of
  /// document is available in the `BulkWriteResult` object.
  var (ret, _) = await collection.bulkWrite([
    {
      /// Insert many is specific to the mongo_dart driver.
      /// The mongo shell does not have this method
      /// It is similar to the `insertMany` method, with the difference
      /// that here we have no limit of document numbers, while the `insertMany`
      /// is limited depending on the MongoDb version (recent releases
      /// set this limit to 100,000 documents).
      ///
      /// You can use the convenient constant (here bulkInsertMany)
      /// or a simple string ('insertMany').
      bulkInsertMany: {
        bulkDocuments: [
          {'cust_num': 99999, 'item': 'abc123', 'status': 'A'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'A'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
          {'cust_num': 81237, 'item': 'sample', 'status': 'A'},
          {'cust_num': 99999, 'item': 'book1', 'status': 'D'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 12911, 'item': 'sample', 'status': 'A'},
          {'cust_num': 12345, 'item': 'book1', 'status': 'A'},
          {'cust_num': 81237, 'item': 'abc123', 'status': 'A'},
          {'cust_num': 12911, 'item': 'sample', 'status': 'D'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'R'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 99999, 'item': 'tst24', 'status': 'S'},
          {'cust_num': 99999, 'item': 'abc123', 'status': 'D'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 12911, 'item': 'sample', 'status': 'D'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 81237, 'item': 'book1', 'status': 'A'},
          {'cust_num': 99999, 'item': 'abc123', 'status': 'D'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'R'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'A'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
          {'cust_num': 81237, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'S'},
          {'cust_num': 12345, 'item': 'sample', 'status': 'D'},
          {'cust_num': 12911, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 12345, 'item': 'book1', 'status': 'D'},
          {'cust_num': 99999, 'item': 'tst24', 'status': 'A'},
          {'cust_num': 81237, 'item': 'abc123', 'status': 'D'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'A'},
          {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
          {'cust_num': 12911, 'item': 'book1', 'status': 'A'},
          {'cust_num': 12345, 'item': 'sample', 'status': 'R'},
        ]
      }
    },
    {
      /// here also you can use the convenient constants or the string
      /// like in Mongo shell
      bulkUpdateMany: {
        bulkFilter: {'status': 'D'},
        bulkUpdate: {
          r'$set': {'status': 'd'}
        }
      }
    },
    {
      bulkUpdateOne: {
        bulkFilter: {'cust_num': 99999, 'item': 'abc123', 'status': 'A'},
        bulkUpdate: {
          r'$inc': {'ordered': 1}
        }
      }
    },
    {
      bulkReplaceOne: {
        bulkFilter: {'cust_num': 12345, 'item': 'tst24', 'status': 'D'},
        bulkReplacement: {
          'cust_num': 12345,
          'item': 'tst24',
          'status': 'Replaced'
        },
        bulkUpsert: true
      }
    }

    /// If ordered, operations will be executed in the exact order in which
    /// you have declared them, and an error will stop the execution
    /// avoiding to process the remaining ones.
    /// Note
    /// The default is true, so the below parameter is our case is not
    /// mandatory
  ], ordered: true);

  print(ret.ok); // 1.0
  print(ret.operationSucceeded); // true
  print(ret.hasWriteErrors); // false
  print(ret.hasWriteConcernError); // false
  print(ret.nInserted); // 35
  print(ret.nUpserted); // 1
  print(ret.nModified); // 14
  print(ret.nMatched); // 15
  print(ret.nRemoved); // 0
  print(ret.isSuccess); // true

  await client.close();
}
