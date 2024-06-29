import '../base/find_and_modify_operation.dart';
import 'find_and_modify_options_v1.dart';

base class FindAndModifyOperationV1 extends FindAndModifyOperation {
  FindAndModifyOperationV1(super.collection,
      {super.query,
      super.sort,
      super.remove,
      required super.update,
      super.returnNew,
      super.fields,
      super.upsert,
      super.arrayFilters,
      super.session,
      super.hint,
      FindAndModifyOptionsV1? findAndModifyOptionsV1,
      super.rawOptions})
      : super.protected(findAndModifyOptions: findAndModifyOptionsV1);
}
