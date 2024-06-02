import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/insert_many/v1/insert_many_options_v1.dart';

base class InsertManyOperationV1 extends InsertManyOperation {
  InsertManyOperationV1(super.collection, super.document,
      {super.session, InsertManyOptionsV1? insertManyOptions, super.rawOptions})
      : super.protected(insertManyOptions: insertManyOptions);
}
