import 'package:mongo_db_driver/src/command/command.dart';

class DeleteManyStatementOpen extends DeleteManyStatement {
  DeleteManyStatementOpen(super.filter, {super.collation, super.hint})
      : super.protected();
}
