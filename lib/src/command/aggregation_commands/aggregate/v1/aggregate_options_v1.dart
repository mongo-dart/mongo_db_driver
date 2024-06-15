import 'package:mongo_db_driver/mongo_db_driver.dart';

class AggregateOptionsV1 extends AggregateOptions {
  AggregateOptionsV1(
      {super.allowDiskUse,
      super.maxTimeMS,
      super.bypassDocumentValidation = false,
      super.readConcern,
      super.collation,
      super.comment,
      super.writeConcern})
      : super.protected();
}
