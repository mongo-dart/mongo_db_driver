import '../base/aggregate_operation.dart';

/// Collection is the collection on which the operation is performed
/// In case of admin/diagnostic pipeline which does not require an underlying
/// collection, the db parameter must be passed instead.
base class AggregateOperationOpen extends AggregateOperation {
  AggregateOperationOpen(super.pipeline,
      {super.collection,
      super.db,
      super.explain,
      super.cursor,
      super.session,
      super.hint,
      super.aggregateOptions,
      super.rawOptions,
      super.let})
      : super.protected();
}
