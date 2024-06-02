import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/delete_one/open/delete_one_options_open.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/delete_one/open/delete_one_statement_open.dart';

base class DeleteOneOperationOpen extends DeleteOneOperation {
  DeleteOneOperationOpen(
      super.collection, DeleteOneStatementOpen super.deleteOneStatement,
      {super.session,
      DeleteOneOptionsOpen? super.deleteOneOptions,
      super.rawOptions})
      : super.protected();
}
