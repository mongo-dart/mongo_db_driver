import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/delete_one/v1/delete_one_statement_v1.dart';

import 'delete_one_options_v1.dart';

base class DeleteOneOperationV1 extends DeleteOneOperation {
  DeleteOneOperationV1(
      super.collection, DeleteOneStatementV1 super.deleteOneStatement,
      {super.session,
      DeleteOneOptionsV1? super.deleteOneOptions,
      super.rawOptions})
      : super.protected();
}
