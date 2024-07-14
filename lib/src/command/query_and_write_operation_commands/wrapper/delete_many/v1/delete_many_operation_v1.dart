import '../../../../command_exp.dart' show DeleteManyOperation;
import 'delete_many_options_v1.dart';
import 'delete_many_statement_v1.dart';

base class DeleteManyOperationV1 extends DeleteManyOperation {
  DeleteManyOperationV1(
      super.collection, DeleteManyStatementV1 super.deleteManyStatement,
      {super.session,
      DeleteManyOptionsV1? super.deleteManyOptions,
      super.rawOptions})
      : super.protected();
}
