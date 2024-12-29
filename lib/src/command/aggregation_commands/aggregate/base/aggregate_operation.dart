import 'package:meta/meta.dart';

import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../command/base/operation_base.dart';
import '../../../../client/client_exp.dart';
import '../../../../database/database_exp.dart';
import '../../../../session/client_session.dart';
import '../../../../topology/server.dart';
import '../../../../unions/hint_union.dart';
import '../../../base/command_operation.dart';
import '../../../command_exp.dart';
import '../open/aggregate_operation_open.dart';
import '../v1/aggregate_operation_v1.dart';
import '../aggregate_result.dart';

/// Collection is the collection on which the operation is performed
/// In case of admin/diagnostic pipeline which does not require an underlying
/// collection, the db parameter must be passed instead.
base class AggregateOperation extends CommandOperation {
  @protected
  AggregateOperation.protected(Object pipeline,
      {MongoCollection? collection,
      MongoDatabase? db,
      bool? explain,
      MongoDocument? cursor,
      super.session,
      this.hint,
      AggregateOptions? aggregateOptions,
      Options? rawOptions,
      this.let})
      : cursor = cursor ?? <String, Object>{},
        explain = explain ?? false,
        super(
            collection?.db ??
                db ??
                (throw MongoDartError('At least a Db must be specified')),
            {},
            <String, dynamic>{
              ...?aggregateOptions?.getOptions(collection?.db ?? db),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.readOperation) {
    if (pipeline is List<Map<String, dynamic>>) {
      this.pipeline = <Map<String, dynamic>>[...pipeline];
    } else if (pipeline is AggregationPipelineBuilder) {
      this.pipeline = pipeline.build();
    } else {
      throw MongoDartError('Received pipeline is "${pipeline.runtimeType}", '
          'while the method only accept "AggregationPipelineBuilder" or '
          '"List<Map<String, Object>>" objects');
    }
  }

  factory AggregateOperation(Object pipeline,
      {MongoCollection? collection,
      MongoDatabase? db,
      bool? explain,
      MongoDocument? cursor,
      ClientSession? session,
      HintUnion? hint,
      AggregateOptions? aggregateOptions,
      Options? rawOptions,
      MongoDocument? let}) {
    if (collection?.serverApi != null) {
      switch (collection!.serverApi!.version) {
        case ServerApiVersion.v1:
          return AggregateOperationV1(pipeline,
              collection: collection,
              db: db,
              explain: explain,
              cursor: cursor,
              session: session,
              hint: hint,
              aggregateOptionsV1: aggregateOptions?.toV1,
              rawOptions: rawOptions,
              let: let);
        // ignore: unreachable_switch_default
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return AggregateOperationOpen(pipeline,
        collection: collection,
        db: db,
        explain: explain,
        cursor: cursor,
        session: session,
        hint: hint,
        aggregateOptionsOpen: aggregateOptions?.toOpen,
        rawOptions: rawOptions,
        let: let);
  }

  /// An array of aggregation pipeline stages that process and transform
  /// the document stream as part of the aggregation pipeline.
  late List<MongoDocument> pipeline;

  /// Specifies to return the information on the processing of the pipeline.
  ///
  /// **Not available in multi-document transactions.**
  bool explain;

  /// Specify a document that contains options that control the creation of
  /// the cursor object.
  ///
  /// Changed in version 3.6: MongoDB 3.6 removes the use of aggregate command
  /// without the cursor option unless the command includes the explain option.
  /// Unless you include the explain option, you must specify the cursor option.
  ///
  /// To indicate a cursor with the default batch size, specify `cursor: {}`.
  /// To indicate a cursor with a non-default batch size, use
  /// `cursor: { batchSize: -num- }`.
  MongoDocument cursor;

  /// Optional. Index specification. Specify either the index name
  /// as a string or the index key pattern.
  /// If specified, then the query system will only consider plans
  /// using the hinted index.
  /// **starting in MongoDB 4.2**, with the following exception,
  /// hint is required if the command includes the min and/or max fields;
  /// hint is not required with min and/or max if the filter is an
  /// equality condition on the _id field { _id: -value- }.
  HintUnion? hint;

  /// Optional. Specifies a document with a list of variables.
  /// This allows you to improve command readability by separating the
  /// variables from the query text.
  /// ``` dart
  /// The document syntax is:
  /// {
  ///   -variable_name_1-: -expression_1-,
  ///   ...,
  ///   -variable_name_n-: -expression_n-
  /// }
  /// ```
  /// The variable is set to the value returned by the expression, and cannot
  /// be changed afterwards.
  ///
  /// To access the value of a variable in the command, use the double dollar
  /// sign prefix ($$) together with your variable name in the form
  /// `$$<variable_name>`. For example: $$targetTotal.
  ///
  /// **Note**
  ///
  /// To use a variable to filter results in a pipeline $match stage,
  /// you must access the variable within the $expr operator.
  ///
  /// New in version 5.0.
  MongoDocument? let;

  @override
  Command $buildCommand() {
    // on null collections (only aggregate) the query is performed
    // on the admin database
    if (collection == null) {
      options[keyDbName] = 'admin';
    }
    return <String, dynamic>{
      keyAggregate: collection?.collectionName ?? 1,
      keyPipeline: pipeline,
      if (explain) keyExplain: explain else keyCursor: cursor,
      if (hint != null && !hint!.isNull) keyHint: hint!.value,
      if (let != null) keyLet: let
    };
  }

  Future<AggregateResult> executeDocument(Server server,
      {ClientSession? session}) async {
    var result = await super.process();
    return AggregateResult(result, session ?? super.session);
  }
}
