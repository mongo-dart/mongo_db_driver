import 'package:mongo_db_driver/src/command/command_exp.dart';

class DeleteManyStatementV1 extends DeleteManyStatement {
  DeleteManyStatementV1(super.filter, {super.collation, super.hint})
      : super.protected();
}
