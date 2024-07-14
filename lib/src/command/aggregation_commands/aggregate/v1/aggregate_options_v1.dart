import '../../../command_exp.dart' show AggregateOptions;

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
