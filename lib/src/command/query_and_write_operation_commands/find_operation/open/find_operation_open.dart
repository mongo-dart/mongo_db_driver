import '../../../../../mongo_db_driver.dart';
import 'find_options_open.dart';

base class FindOperationOpen extends FindOperation {
  FindOperationOpen(super.collection, super.filter,
      {super.sort,
      super.projection,
      super.hint,
      super.skip,
      super.limit,
      super.session,
      FindOptionsOpen? findOptions,
      super.rawOptions})
      : super.protected(findOptions: findOptions);
}
