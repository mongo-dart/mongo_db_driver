import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../core/error/mongo_dart_error.dart';
import '../../../../database/base/mongo_collection.dart';
import '../../../../database/server_api_version.dart';
import '../../../../session/client_session.dart';
import '../../../../unions/hint_union.dart';
import '../../../../unions/projection_union.dart';
import '../../../../unions/query_union.dart';
import '../../../../unions/sort_union.dart';
import '../../update_operation/base/update_union.dart';
import '../open/find_and_modify_operation_open.dart';
import '../v1/find_and_modify_operation_v1.dart';
import 'find_and_modify_options.dart';
import '../../../base/command_operation.dart';
import '../../return_classes/find_and_modify_result.dart';

typedef FindAndModifyDocumentRec = (
  FindAndModifyResult findAndModifyResult,
  MongoDocument serverDocument
);

base class FindAndModifyOperation extends CommandOperation {
  FindAndModifyOperation.protected(MongoCollection collection,
      {QueryUnion? query,
      this.sort,
      bool? remove,
      this.update,
      bool? returnNew,
      this.fields,
      bool? upsert,
      this.arrayFilters,
      super.session,
      this.hint,
      FindAndModifyOptions? findAndModifyOptions,
      Options? rawOptions})
      : query = query ?? QueryUnion(emptyQueryFilter),
        remove = remove ?? false,
        returnNew = returnNew ?? false,
        upsert = upsert ?? false,
        super(
            collection.db,
            {},
            <String, dynamic>{
              ...?findAndModifyOptions?.getOptions(collection.db),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.writeOperation) {
    if (arrayFilters != null && arrayFilters is! List && arrayFilters is! Map) {
      throw MongoDartError(
          'The arrayFilters parameter must be either a List or a Map');
    }
  }

  factory FindAndModifyOperation(MongoCollection collection,
      {QueryUnion? query,
      SortUnion? sort,
      bool? remove,
      UpdateUnion? update,
      bool? returnNew,
      ProjectionUnion? fields,
      bool? upsert,
      List<ArrayFilter>? arrayFilters,
      ClientSession? session,
      HintUnion? hint,
      FindAndModifyOptions? findAndModifyOptions,
      Options? rawOptions}) {
    query ??= QueryUnion(emptyQueryFilter);
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return FindAndModifyOperationV1(collection,
              query: query,
              sort: sort,
              remove: remove,
              update: update,
              returnNew: returnNew,
              fields: fields,
              upsert: upsert,
              arrayFilters: arrayFilters,
              session: session,
              findAndModifyOptionsV1: findAndModifyOptions?.toFindAndModifyV1,
              rawOptions: rawOptions);
        // ignore: unreachable_switch_default
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return FindAndModifyOperationOpen(collection,
        query: query,
        sort: sort,
        remove: remove,
        update: update,
        returnNew: returnNew,
        fields: fields,
        upsert: upsert,
        arrayFilters: arrayFilters,
        session: session,
        findAndModifyOptionsOpen: findAndModifyOptions?.toFindAndModifyOpen,
        rawOptions: rawOptions);
  }

  /// The selection criteria for the modification. The query field employs
  /// the same query selectors as used in the db.collection.find() method.
  /// Although the query may match multiple documents,
  /// findAndModify will only select one document to modify.
  ///
  /// If unspecified, defaults to an empty document.
  ///
  /// Starting in MongoDB 4.2 (and 4.0.12+, 3.6.14+, and 3.4.23+),
  /// the operation errors if the query argument is not a document.
  QueryUnion query;

  /// Determines which document the operation modifies if the query selects
  /// multiple documents. findAndModify modifies the first document
  /// in the sort order specified by this argument.
  ///
  /// Starting in MongoDB 4.2 (and 4.0.12+, 3.6.14+, and 3.4.23+),
  /// the operation errors if the sort argument is not a document.
  ///
  /// In MongoDB, sorts are inherently stable, unless sorting on a field
  /// which contains duplicate values:
  /// - a stable sort is one that returns the same sort order each time
  ///   it is performed
  /// - an unstable sort is one that may return a different sort order
  ///   when performed multiple times
  ///
  /// If a stable sort is desired, include at least one field in your sort
  /// that contains exclusively unique values. The easiest way to guarantee
  /// this is to include the _id field in your sort query.
  ///
  /// See [Sort Stability](https://docs.mongodb.com/manual/reference/method/cursor.sort/#sort-cursor-stable-sorting) for more information.
  SortUnion? sort;

  /// Must specify either the remove or the update field. Removes the document
  /// specified in the query field. Set this to true to remove the
  /// selected document . The default is false.
  bool remove;

  /// Must specify either the remove or the update field.
  /// Performs an update of the selected document.
  ///
  /// - If passed a document with update operator expressions,
  /// findAndModify performs the specified modification.
  /// - If passed a replacement document { -field1-: -value1-, ...},
  /// the findAndModify performs a replacement.
  /// - starting in MongoDB 4.2, if passed an aggregation pipeline
  /// [ -stage1-, -stage2-, ... ], findAndModify modifies the document
  /// per the pipeline. The pipeline can consist of the following stages:
  ///   * $addFields and its alias $set
  ///   * $project and its alias $unset
  ///   * $replaceRoot and its alias $replaceWith.
  ///
  /// It can be a Map or a List
  UpdateUnion? update;

  /// When true, returns the modified document rather than the original.
  /// The findAndModify method ignores the 'new' option for remove operations.
  /// The default is false.
  ///
  /// Original name `new` renamed in `returnNew` because of the reserved word
  bool returnNew;

  /// A subset of fields to return. The fields document specifies an inclusion
  /// of a field with 1, as in: fields: { -field1-: 1, -field2-: 1, ... }.
  /// See [Projection](https://docs.mongodb.com/manual/reference/method/db.collection.find/#find-projection).
  ///
  /// Starting in MongoDB 4.2 (and 4.0.12+, 3.6.14+, and 3.4.23+),
  /// the operation errors if the fields argument is not a document.
  ProjectionUnion? fields;

  /// Used in conjunction with the update field.
  ///
  /// When true, findAndModify() either:
  /// - Creates a new document if no documents match the query.
  /// For more details see [upsert behavior](https://docs.mongodb.com/manual/reference/method/db.collection.update/#upsert-behavior).
  /// - Updates a single document that matches the query.
  ///
  /// To avoid multiple upserts,
  /// ensure that the query fields are uniquely indexed.
  ///
  /// Defaults to false.
  bool upsert;

  /// An array of filter documents that determine which array elements to
  /// modify for an update operation on an array field.
  ///
  /// In the update document, use the `$[<identifier>]` filtered positional
  /// operator to define an identifier, which you then reference in the
  /// array filter documents. You cannot have an array filter document
  /// for an identifier if the identifier is not included in the
  /// update document.
  ///
  /// **NOTE**
  /// The `<identifier>` must begin with a lowercase letter and contain
  /// only alphanumeric characters.
  ///
  /// You can include the same identifier multiple times in the
  /// update document; however, for each distinct identifier **($[identifier])**
  /// in the update document, you must specify **exactly one** corresponding array
  /// filter document. That is, you cannot specify multiple array filter
  /// documents for the same identifier. For example, if the update statement
  /// includes the identifier x (possibly multiple times),
  /// you cannot specify the following for **arrayFilters** that includes 2
  /// separate filter documents for x:
  /// ```dart
  /// // INVALID
  /// [
  ///   { "x.a": { $gt: 85 } },
  ///   { "x.b": { $gt: 80 } }
  /// ]
  /// ```
  /// However, you can specify compound conditions on the same identifier
  /// in a single filter document, such as in the following examples:
  /// ```dart
  /// // Example 1
  /// [
  ///   { $or: [{"x.a": {$gt: 85}}, {"x.b": {$gt: 80}}] }
  /// ]
  ///
  /// // Example 2
  /// [
  ///   { $and: [{"x.a": {$gt: 85}}, {"x.b": {$gt: 80}}] }
  /// ]
  /// // Example 3
  /// [
  ///   { "x.a": { $gt: 85 }, "x.b": { $gt: 80 } }
  /// ]
  /// ```
  /// For examples, see [Array Update Operations with arrayFilters](https://docs.mongodb.com/manual/reference/command/findAndModify/#findandmodify-command-arrayfilters).
  ///
  /// **NOTE**
  /// **arrayFilters** is not available for updates that use an
  /// aggregation pipeline.
  ///
  /// New in version 3.6.
  ///
  List<ArrayFilter>? arrayFilters;

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
        keyFindAndModify: collection!.collectionName,
        if (!query.isNull) keyQuery: query.query,
        if (sort != null) keySort: sort!.sort,
        if (remove) keyRemove: remove,
        if (update != null && !update!.isNull && !update!.specs.isNull)
          keyUpdate: update!.specs.value,
        if (returnNew) keyNew: returnNew,
        if (fields != null) keyFields: fields!.projection,
        if (upsert) keyUpsert: upsert,
        if (arrayFilters != null) keyArrayFilters: arrayFilters!,
        if (hint != null && !hint!.isNull) keyHint: hint!.value
      };

  /*       Future<MongoDocument> executeFindAndModify() async => process(); 

  Future<FindAndModifyDocumentRec> executeDocument() async {
        var ret= await executeFindAndModify( );

    return (FindAndModifyResult(ret),ret);
  } */
}
