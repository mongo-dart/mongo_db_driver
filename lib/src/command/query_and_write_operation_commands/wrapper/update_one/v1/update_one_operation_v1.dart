import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/update_one/v1/update_one_statement_v1.dart';

import 'update_one_options_v1.dart';

base class UpdateOneOperationV1 extends UpdateOneOperation {
  UpdateOneOperationV1(
      super.collection, UpdateOneStatementV1 super.updateOneStatement,
      {super.session,
      UpdateOneOptionsV1? super.updateOneOptions,
      super.rawOptions})
      : super.protected();
}
