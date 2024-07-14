import '../../../../command_exp.dart' show DeleteManyOperation;
import 'delete_many_options_open.dart';
import 'delete_many_statement_open.dart';

base class DeleteManyOperationOpen extends DeleteManyOperation {
  DeleteManyOperationOpen(
      super.collection, DeleteManyStatementOpen super.deleteManyStatement,
      {super.session,
      DeleteManyOptionsOpen? super.deleteManyOptions,
      super.rawOptions})
      : super.protected();
}
