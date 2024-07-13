import 'package:mongo_db_driver/src/command/command_exp.dart';

class ReplaceOneStatementOpen extends ReplaceOneStatement {
  ReplaceOneStatementOpen(super.q, super.u,
      {super.upsert, super.collation, super.hint})
      : super.protected();
}
