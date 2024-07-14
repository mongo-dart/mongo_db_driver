import '../../../../command_exp.dart';
import 'update_one_options_v1.dart';

base class UpdateOneOperationV1 extends UpdateOneOperation {
  UpdateOneOperationV1(
      super.collection, UpdateOneStatementV1 super.updateOneStatement,
      {super.session,
      UpdateOneOptionsV1? super.updateOneOptions,
      super.rawOptions})
      : super.protected();
}
