import 'package:mongo_db_driver/mongo_db_driver.dart';

import 'update_many_options_v1.dart';
import 'update_many_statement_v1.dart';

base class UpdateManyOperationV1 extends UpdateManyOperation {
  UpdateManyOperationV1(
      super.collection, UpdateManyStatementV1 super.updateManyStatement,
      {super.ordered,
      super.session,
      UpdateManyOptionsV1? super.updateManyOptions,
      super.rawOptions})
      : super.protected();
}
