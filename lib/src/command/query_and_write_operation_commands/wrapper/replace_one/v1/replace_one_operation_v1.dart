import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/replace_one/v1/replace_one_statement_v1.dart';

import 'replace_one_options_v1.dart';

base class ReplaceOneOperationV1 extends ReplaceOneOperation {
  ReplaceOneOperationV1(
      super.collection, ReplaceOneStatementV1 super.replaceOneStatement,
      {super.session,
      ReplaceOneOptionsV1? super.replaceOneOptions,
      super.rawOptions})
      : super.protected();
}
