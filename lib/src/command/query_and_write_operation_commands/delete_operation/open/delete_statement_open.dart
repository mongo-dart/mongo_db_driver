import '../base/delete_statement.dart';

class DeleteStatementOpen extends DeleteStatement {
  DeleteStatementOpen(super.filter, {super.collation, super.hint, super.limit})
      : super.protected();
}
