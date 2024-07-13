import 'package:mongo_db_driver/src/command/command_exp.dart';

class UpdateOneStatementOpen extends UpdateOneStatement {
  UpdateOneStatementOpen(super.q, super.u,
      {super.upsert, super.collation, super.arrayFilters, super.hint})
      : super.protected();
}
