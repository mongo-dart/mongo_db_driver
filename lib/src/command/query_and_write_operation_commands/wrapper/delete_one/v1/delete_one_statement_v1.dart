import 'package:mongo_db_driver/src/command/command.dart';

class DeleteOneStatementV1 extends DeleteOneStatement {
  DeleteOneStatementV1(super.filter, {super.collation, super.hint})
      : super.protected();
}
