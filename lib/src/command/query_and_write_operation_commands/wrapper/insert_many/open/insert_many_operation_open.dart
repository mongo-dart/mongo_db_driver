import 'package:mongo_db_driver/mongo_db_driver.dart';

import 'insert_many_options_open.dart';

base class InsertManyOperationOpen extends InsertManyOperation {
  InsertManyOperationOpen(super.collection, super.document,
      {super.session,
      InsertManyOptionsOpen? insertManyOptions,
      super.rawOptions})
      : super.protected(insertManyOptions: insertManyOptions);
}
