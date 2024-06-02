import 'package:mongo_db_driver/mongo_db_driver.dart';

import 'insert_one_options_v1.dart';

base class InsertOneOperationV1 extends InsertOneOperation {
  InsertOneOperationV1(super.collection, super.document,
      {super.session, InsertOneOptionsV1? insertOneOptions, super.rawOptions})
      : super.protected(insertOneOptions: insertOneOptions);
}
