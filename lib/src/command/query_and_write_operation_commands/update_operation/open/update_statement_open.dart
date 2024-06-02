import 'package:mongo_db_driver/src/command/command.dart';

class UpdateStatementOpen extends UpdateStatement {
  UpdateStatementOpen(super.q, super.u,
      {super.upsert,
      super.multi,
      super.collation,
      super.arrayFilters,
      super.hint})
      : super.protected();
}
