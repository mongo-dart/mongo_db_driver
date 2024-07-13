import 'package:mongo_db_driver/src/command/command_exp.dart';

class UpdateOneStatementV1 extends UpdateOneStatement {
  UpdateOneStatementV1(super.q, super.u,
      {super.upsert, super.collation, super.arrayFilters, super.hint})
      : super.protected();
}
