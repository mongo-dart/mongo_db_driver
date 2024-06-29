import '../base/find_and_modify_operation.dart';
import 'find_and_modify_options_open.dart';

base class FindAndModifyOperationOpen extends FindAndModifyOperation {
  FindAndModifyOperationOpen(super.collection,
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
      FindAndModifyOptionsOpen? findAndModifyOptionsOpen,
      super.rawOptions})
      : super.protected(findAndModifyOptions: findAndModifyOptionsOpen);
}
