import '../../../../command_exp.dart';
import 'delete_one_options_v1.dart';

base class DeleteOneOperationV1 extends DeleteOneOperation {
  DeleteOneOperationV1(
      super.collection, DeleteOneStatementV1 super.deleteOneStatement,
      {super.session,
      DeleteOneOptionsV1? super.deleteOneOptions,
      super.rawOptions})
      : super.protected();
}
