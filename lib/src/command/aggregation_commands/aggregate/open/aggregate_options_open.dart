import 'package:mongo_db_driver/mongo_db_driver.dart';

class AggregateOptionsOpen extends AggregateOptions {
  AggregateOptionsOpen(
      {super.allowDiskUse,
      super.maxTimeMS,
      super.bypassDocumentValidation = false,
      super.readConcern,
      super.collation,
      super.comment,
      super.writeConcern})
      : super.protected();
}
