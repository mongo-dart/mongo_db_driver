import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/delete_many/v1/delete_many_statement_v1.dart';
import 'package:mongo_db_driver/src/unions/hint_union.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../command/aggregation_commands/aggregate/v1/aggregate_operation_v1.dart';
import '../../command/base/operation_base.dart';
import '../../command/command.dart';
import '../../command/query_and_write_operation_commands/find_operation/v1/find_operation_v1.dart';
import '../../command/query_and_write_operation_commands/update_operation/base/update_union.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_many/v1/delete_many_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_one/v1/delete_one_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_one/v1/delete_one_statement_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/v1/find_one_and_delete_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/v1/find_one_and_replace_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/v1/find_one_and_update_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/replace_one/v1/replace_one_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/replace_one/v1/replace_one_statement_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_many/v1/update_many_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_many/v1/update_many_statement_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_one/v1/update_one_operation_v1.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_one/v1/update_one_statement_v1.dart';
import '../../session/client_session.dart';
import '../../unions/query_union.dart';
import '../database.dart'
    hide MongoDocument, ProjectionDocument, IndexDocument, ArrayFilter;
import '../cursor.dart';

/// Collection class for Stable Api V1
class MongoCollectionV1 extends MongoCollection {
  MongoCollectionV1(super.db, super.collectionName) : super.protected();

  // Insert one document into this collection
  // Returns a WriteResult object
  @override
  Future<InsertOneDocumentRec> insertOne(MongoDocument document,
          {ClientSession? session,
          InsertOneOptions? insertOneOptions,
          Options? rawOptions}) async =>
      InsertOneOperationV1(this, document,
              session: session,
              insertOneOptions: insertOneOptions?.toOneV1,
              rawOptions: rawOptions)
          .executeDocument();

  /// Insert many document into this collection
  /// Returns a BulkWriteResult object
  @override
  Future<InsertManyDocumentRec> insertMany(List<MongoDocument> documents,
          {ClientSession? session,
          InsertManyOptions? insertManyOptions,
          Options? rawOptions}) async =>
      InsertManyOperationV1(this, documents,
              session: session,
              insertManyOptions: insertManyOptions?.toManyV1,
              rawOptions: rawOptions)
          .executeDocument();

  // Update one document into this collection
  @override
  Future<UpdateOneDocumentRec> updateOne(filter, update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      List<dynamic>? arrayFilters,
      HintUnion? hint,
      UpdateOneOptions? updateOneOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter = filter is QueryUnion ? filter : QueryUnion(filter);
    UpdateUnion uUpdate = update is UpdateUnion ? update : UpdateUnion(update);

    var updateOneOperation = UpdateOneOperationV1(
        this,
        UpdateOneStatementV1(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateOneOptions:
            UpdateOneOptions(writeConcern: writeConcern).toUpdateOneV1,
        rawOptions: rawOptions);
    return updateOneOperation.executeDocument();
  }

  @override
  Future<ReplaceOneDocumentRec> replaceOne(filter, MongoDocument update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      HintUnion? hint,
      ReplaceOneOptions? replaceOneOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter = filter is QueryUnion ? filter : QueryUnion(filter);

    var replaceOneOperation = ReplaceOneOperationV1(
        this,
        ReplaceOneStatementV1(uFilter, update,
            upsert: upsert, collation: collation, hint: hint),
        session: session,
        replaceOneOptions:
            ReplaceOneOptions(writeConcern: writeConcern).toReplaceOneV1,
        rawOptions: rawOptions);
    return replaceOneOperation.executeDocument();
  }

  @override
  Future<UpdateManyDocumentRec> updateMany(selector, update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      List<dynamic>? arrayFilters,
      ClientSession? session,
      HintUnion? hint,
      UpdateManyOptions? updateManyOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter =
        selector is QueryUnion ? selector : QueryUnion(selector);
    UpdateUnion uUpdate = update is UpdateUnion ? update : UpdateUnion(update);
    updateManyOptions?.writeConcern ??= writeConcern;
    var updateManyOperation = UpdateManyOperationV1(
        this,
        UpdateManyStatementV1(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateManyOptions: updateManyOptions?.toUpdateManyV1 ??
            UpdateManyOptions(writeConcern: writeConcern).toUpdateManyV1,
        rawOptions: rawOptions);
    return updateManyOperation.executeDocument();
  }

  @override
  Future<DeleteOneDocumentRec> deleteOne(selector,
      {WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      HintUnion? hint,
      DeleteOneOptions? deleteOneOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter =
        selector is QueryUnion ? selector : QueryUnion(selector);
    var deleteOperation = DeleteOneOperationV1(
        this, DeleteOneStatementV1(uFilter, collation: collation, hint: hint),
        session: session,
        deleteOneOptions: deleteOneOptions?.toDeleteOneV1 ??
            DeleteOneOptions(writeConcern: writeConcern).toDeleteOneV1,
        rawOptions: rawOptions);
    return deleteOperation.executeDocument();
  }

  @override
  Future<DeleteManyDocumentRec> deleteMany(
      {selector,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      HintUnion? hint,
      DeleteManyOptions? deleteManyOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter =
        selector is QueryUnion ? selector : QueryUnion(selector);
    var deleteOperation = DeleteManyOperationV1(
        this, DeleteManyStatementV1(uFilter, collation: collation, hint: hint),
        session: session,
        deleteManyOptions: deleteManyOptions?.toDeleteManyV1 ??
            DeleteManyOptions(writeConcern: writeConcern).toDeleteManyV1,
        rawOptions: rawOptions);
    return deleteOperation.executeDocument();
  }

  @override
  Future<FindOneAndDeleteDocumentRec> findOneAndDelete(query,
      {dynamic fields,
      dynamic sort,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndDeleteOptions? findOneAndDeleteOptions,
      Options? rawOptions}) async {
    QueryExpression? queryExpression = query is QueryExpression ? query : null;
    var uFilter = (query is QueryUnion) ? query : QueryUnion(query);
    var uProjection = detectProjectUnion(fields, queryExpression);
    var uSort = detectSortUnion(sort, queryExpression);
    var uHint = (hint is HintUnion) ? hint : HintUnion(hint);

    var famOperation = FindOneAndDeleteOperationV1(this, uFilter,
        fields: uProjection,
        sort: uSort,
        hint: uHint,
        findOneAndDeleteOptions:
            findOneAndDeleteOptions?.toFindOneAndDeleteOptionsV1,
        rawOptions: rawOptions);
    return famOperation.executeDocument();
  }

  @override
  Future<FindOneAndReplaceDocumentRec> findOneAndReplace(
      query, MongoDocument replacement,
      {dynamic fields,
      dynamic sort,
      bool? upsert,
      bool? returnNew,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndReplaceOptions? findOneAndReplaceOptions,
      Options? rawOptions}) async {
    QueryExpression? queryExpression = query is QueryExpression ? query : null;
    var uFilter = (query is QueryUnion) ? query : QueryUnion(query);
    var uProjection = detectProjectUnion(fields, queryExpression);
    var uSort = detectSortUnion(sort, queryExpression);
    var uHint = (hint is HintUnion) ? hint : HintUnion(hint);

    var famOperation = FindOneAndReplaceOperationV1(this, uFilter, replacement,
        fields: uProjection,
        sort: uSort,
        returnNew: returnNew,
        upsert: upsert,
        session: session,
        hint: uHint,
        findOneAndReplaceOptions:
            findOneAndReplaceOptions?.toFindOneAndReplaceOptionsV1,
        rawOptions: rawOptions);
    return famOperation.executeDocument();
  }

  @override
  Future<FindOneAndUpdateDocumentRec> findOneAndUpdate(query, update,
      {dynamic fields,
      dynamic sort,
      bool? upsert,
      bool? returnNew,
      List<ArrayFilter>? arrayFilters,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndUpdateOptions? findOneAndUpdateOptions,
      Options? rawOptions}) async {
    QueryExpression? queryExpression = query is QueryExpression ? query : null;
    var uFilter = (query is QueryUnion) ? query : QueryUnion(query);
    var uProjection = detectProjectUnion(fields, queryExpression);
    var uSort = detectSortUnion(sort, queryExpression);
    var uHint = (hint is HintUnion) ? hint : HintUnion(hint);

    var famOperation = FindOneAndUpdateOperationV1(this,
        query: uFilter,
        update: UpdateUnion(update),
        fields: uProjection,
        sort: uSort,
        upsert: upsert,
        returnNew: returnNew,
        arrayFilters: arrayFilters,
        session: session,
        hint: uHint,
        findOneAndUpdateOptions:
            findOneAndUpdateOptions?.toFindOneAndUpdateOptionsV1,
        rawOptions: rawOptions);
    return famOperation.executeDocument();
  }

  /// Returns one document that satisfies the specified query criteria on
  /// the collection or view. If multiple documents satisfy the query,
  /// this method returns the first document according to the sort order
  /// or the natural order of sort parameter is not specified.
  /// In capped collections, natural order is the same as insertion order.
  /// If no document satisfies the query, the method returns null.
  ///
  /// In MongoDb this method only allows the filter and the projection
  /// parameters.
  /// This version has more parameters, and it is essentially a wrapper
  /// araound the find method with a fixed limit set to 1 that returns
  /// a document instead of a stream.
  @override
  Future<Map<String, dynamic>?> findOne(
      {dynamic filter,
      dynamic projection,
      dynamic sort,
      int? skip,
      ClientSession? session,
      dynamic hint,
      FindOptions? findOptions,
      MongoDocument? rawOptions}) async {
    QueryExpression? queryExpression =
        filter is QueryExpression ? filter : null;
    var uFilter = (filter is QueryUnion) ? filter : QueryUnion(filter);
    var uProjection = detectProjectUnion(projection, queryExpression);
    var uSort = detectSortUnion(sort, queryExpression);
    var uHint = (hint is HintUnion) ? hint : HintUnion(hint);
    skip = skip != null && skip > 0
        ? skip
        : ((filter is QueryExpression) && filter.getSkip() > 0
            ? filter.getSkip()
            : null);

    var operation = FindOperationV1(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        skip: skip,
        limit: 1,
        session: session,
        findOptions: findOptions?.toV1,
        rawOptions: rawOptions);

    return Cursor(operation, db.server).nextObject();
  }

  /// Selects documents in a collection or view and returns a stream
  /// of the selected documents.  @override
  @override
  Stream<Map<String, dynamic>> find(
      {dynamic filter,
      dynamic projection,
      dynamic sort,
      int? skip,
      int? limit,
      ClientSession? session,
      dynamic hint,
      FindOptions? findOptions,
      MongoDocument? rawOptions}) {
    QueryExpression? queryExpression =
        filter is QueryExpression ? filter : null;
    var uFilter = (filter is QueryUnion) ? filter : QueryUnion(filter);
    var uProjection = detectProjectUnion(projection, queryExpression);
    var uSort = detectSortUnion(sort, queryExpression);
    var uHint = (hint is HintUnion) ? hint : HintUnion(hint);
    skip = skip != null && skip > 0
        ? skip
        : ((filter is QueryExpression) && filter.getSkip() > 0
            ? filter.getSkip()
            : null);
    limit = limit != null && limit > 0
        ? limit
        : ((filter is QueryExpression) ? filter.getLimit() : 0);

    var operation = FindOperationV1(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        limit: limit,
        skip: skip,
        session: session,
        findOptions: findOptions?.toV1,
        rawOptions: rawOptions);

    return Cursor(operation, db.server).stream;
  }

  // ********************************************
  // ******************** Aggregate *************
  // ********************************************

  /// This method returns a curosr that can be read or transformed into
  /// a stream with `stream` (for a stream you can directly call
  /// `aggregateToStream`)
  ///
  /// The pipeline can be either an `AggregationPipelineBuilder` or a
  /// List of Maps (`List<Map<String, Object>>`)
  @override
  Cursor aggregate(dynamic pipeline,
      {bool? explain,
      Map<String, Object>? cursor,
      HintUnion? hint,
      ClientSession? session,
      AggregateOptions? aggregateOptions,
      Map<String, Object>? rawOptions,
      MongoDocument? let}) {
    return Cursor(
        AggregateOperationV1(pipeline,
            collection: this,
            explain: explain,
            cursor: cursor,
            hint: hint,
            session: session,
            aggregateOptionsV1: aggregateOptions?.toV1,
            rawOptions: rawOptions,
            let: let),
        db.server);
  }
}
