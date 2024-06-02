import 'package:mongo_db_driver/mongo_db_driver.dart';

import 'update_many_options_open.dart';
import 'update_many_statement_open.dart';

base class UpdateManyOperationOpen extends UpdateManyOperation {
  UpdateManyOperationOpen(
      super.collection, UpdateManyStatementOpen super.updateManyStatement,
      {super.ordered,
      super.session,
      UpdateManyOptionsOpen? super.updateManyOptions,
      super.rawOptions})
      : super.protected();
}
