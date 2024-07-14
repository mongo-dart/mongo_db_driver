import '../../../../command_exp.dart';
import 'replace_one_options_v1.dart';

base class ReplaceOneOperationV1 extends ReplaceOneOperation {
  ReplaceOneOperationV1(
      super.collection, ReplaceOneStatementV1 super.replaceOneStatement,
      {super.session,
      ReplaceOneOptionsV1? super.replaceOneOptions,
      super.rawOptions})
      : super.protected();
}
