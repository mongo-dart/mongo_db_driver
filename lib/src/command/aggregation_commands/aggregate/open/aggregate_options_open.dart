import '../../../command_exp.dart' show AggregateOptions;

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
