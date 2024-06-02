import '../base/find_one_and_update_operation.dart';
import 'find_one_and_update_options_open.dart';

base class FindOneAndUpdateOperationOpen extends FindOneAndUpdateOperation {
  FindOneAndUpdateOperationOpen(super.collection,
      {super.query,
      super.update,
      super.fields,
      super.sort,
      super.upsert,
      super.returnNew,
      super.arrayFilters,
      super.session,
      super.hint,
      FindOneAndUpdateOptionsOpen? super.findOneAndUpdateOptions,
      super.rawOptions})
      : super.protected();
}
