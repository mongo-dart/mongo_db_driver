import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../../database/base/mongo_collection.dart';
import '../../../session/client_session.dart';
import '../../../topology/server.dart';
import '../../../unions/hint_union.dart';
import 'count_options.dart';
import '../../base/command_operation.dart';
import 'count_result.dart';

/// Collection is the collection on which the operation is performed
/// In case of admin/diagnostic pipeline which does not require an underlying
/// collection, the db parameter must be passed instead.
base class CountOperation extends CommandOperation {
  CountOperation(MongoCollection collection,
      {this.query,
      this.limit,
      this.skip,
      super.session,
      this.hint,
      CountOptions? countOptions,
      Map<String, Object>? rawOptions})
      : super(collection.db, {},
            <String, dynamic>{...?countOptions?.options, ...?rawOptions},
            collection: collection, aspect: Aspect.readOperation);

  /// A query that selects which documents to count in the collection or view.
  Map<String, dynamic>? query;

  /// The maximum number of matching documents to return.
  int? limit;

  /// The number of matching documents to skip before returning results.
  int? skip;

  /// Optional. Index specification. Specify either the index name
  /// as a string or the index key pattern.
  /// If specified, then the query system will only consider plans
  /// using the hinted index.
  /// **starting in MongoDB 4.2**, with the following exception,
  /// hint is required if the command includes the min and/or max fields;
  /// hint is not required with min and/or max if the filter is an
  /// equality condition on the _id field { _id: -value- }.
  HintUnion? hint;

  @override
  Command $buildCommand() => <String, dynamic>{
        keyCount: collection!.collectionName,
        if (query != null) keyQuery: query!,
        if (limit != null && limit! > 0) keyLimit: limit!,
        if (skip != null && skip! > 0) keySkip: skip!,
        if (hint != null && !hint!.isNull) keyHint: hint!.value,
      };

  Future<CountResult> executeDocument(Server server,
      {ClientSession? session}) async {
    var result = await super.process();
    return CountResult(result);
  }
}
