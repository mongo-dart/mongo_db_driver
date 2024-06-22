import '../base/aggregate_operation.dart';

/// Collection is the collection on which the operation is performed
/// In case of admin/diagnostic pipeline which does not require an underlying
/// collection, the db parameter must be passed instead.
///
/// When using Stable API V1:
/// - You cannot use the following stages in an aggregate command:
///   * $currentOp
///   * $indexStats
///   * $listLocalSessions
///   * $listSessions
///   * $planCacheStats
///   * $search
/// - Don't include the explain field in an aggregate command. If you do,
/// the server returns an APIStrictError error.
/// - When using the $collStats stage, you can only use the count field.
/// No other $collStats fields are available.
base class AggregateOperationV1 extends AggregateOperation {
  AggregateOperationV1(super.pipeline,
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
