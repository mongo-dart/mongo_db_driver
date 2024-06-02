import 'package:mongo_db_driver/src/command/command.dart';

class DeleteOneStatementOpen extends DeleteOneStatement {
  DeleteOneStatementOpen(super.filter, {super.collation, super.hint})
      : super.protected();
}
