import 'package:mongo_db_driver/src/command/command.dart';

class UpdateManyStatementV1 extends UpdateManyStatement {
  UpdateManyStatementV1(super.q, super.u,
      {super.upsert, super.collation, super.arrayFilters, super.hint})
      : super.protected();
}
