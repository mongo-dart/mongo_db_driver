import 'package:mongo_db_driver/src/command/command.dart';

class ReplaceOneStatementV1 extends ReplaceOneStatement {
  ReplaceOneStatementV1(super.q, super.u,
      {super.upsert, super.collation, super.hint})
      : super.protected();
}
