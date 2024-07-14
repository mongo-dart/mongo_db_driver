import '../../../../command_exp.dart' show InsertManyOperation;
import 'insert_many_options_open.dart';

base class InsertManyOperationOpen extends InsertManyOperation {
  InsertManyOperationOpen(super.collection, super.document,
      {super.session,
      InsertManyOptionsOpen? insertManyOptions,
      super.rawOptions})
      : super.protected(insertManyOptions: insertManyOptions);
}
