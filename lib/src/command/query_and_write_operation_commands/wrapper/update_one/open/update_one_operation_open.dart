import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/update_one/open/update_one_options_open.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/update_one/open/update_one_statement_open.dart';

base class UpdateOneOperationOpen extends UpdateOneOperation {
  UpdateOneOperationOpen(
      super.collection, UpdateOneStatementOpen super.updateOneStatement,
      {super.session,
      UpdateOneOptionsOpen? super.updateOneOptions,
      super.rawOptions})
      : super.protected();
}
