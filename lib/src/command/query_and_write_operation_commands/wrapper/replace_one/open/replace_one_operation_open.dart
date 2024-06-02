import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/replace_one/open/replace_one_options_open.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/replace_one/open/replace_one_statement_open.dart';

base class ReplaceOneOperationOpen extends ReplaceOneOperation {
  ReplaceOneOperationOpen(
      super.collection, ReplaceOneStatementOpen super.replaceOneStatement,
      {super.session,
      ReplaceOneOptionsOpen? super.replaceOneOptions,
      super.rawOptions})
      : super.protected();
}
