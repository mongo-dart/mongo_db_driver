import '../base/find_one_and_update_operation.dart';
import 'find_one_and_update_options_v1.dart';

base class FindOneAndUpdateOperationV1 extends FindOneAndUpdateOperation {
  FindOneAndUpdateOperationV1(super.collection,
      {super.query,
      super.update,
      super.fields,
      super.sort,
      super.upsert,
      super.returnNew,
      super.arrayFilters,
      super.session,
      super.hint,
      FindOneAndUpdateOptionsV1? super.findOneAndUpdateOptions,
      super.rawOptions})
      : super.protected();
}
