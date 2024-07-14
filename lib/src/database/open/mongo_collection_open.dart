import 'package:mongo_db_driver/src/unions/hint_union.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../command/aggregation_commands/aggregate/open/aggregate_operation_open.dart';
import '../../command/base/operation_base.dart';
import '../../command/command_exp.dart';
import '../../command/query_and_write_operation_commands/find_operation/open/find_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_many/open/delete_many_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_many/open/delete_many_statement_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_one/open/delete_one_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/delete_one/open/delete_one_statement_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/open/find_one_and_delete_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/open/find_one_and_replace_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/open/find_one_and_update_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/replace_one/open/replace_one_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/replace_one/open/replace_one_statement_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_many/open/update_many_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_many/open/update_many_statement_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_one/open/update_one_operation_open.dart';
import '../../command/query_and_write_operation_commands/wrapper/update_one/open/update_one_statement_open.dart';
import '../../session/client_session.dart';
import '../../unions/query_union.dart';
import '../database_exp.dart';

class MongoCollectionOpen extends MongoCollection {
  MongoCollectionOpen(super.db, super.collectionName) : super.protected();

  // Insert one document into this collection
  // Returns a WriteResult object
  @override
  Future<InsertOneDocumentRec> insertOne(MongoDocument document,
          {ClientSession? session,
          InsertOneOptions? insertOneOptions,
          Options? rawOptions}) async =>
      InsertOneOperationOpen(this, document,
              session: session,
              insertOneOptions: insertOneOptions?.toOneOpen,
              rawOptions: rawOptions)
          .executeDocument();

  /// Insert many document into this collection
  /// Returns a BulkWriteResult object
  @override
  Future<InsertManyDocumentRec> insertMany(List<MongoDocument> documents,
          {ClientSession? session,
          InsertManyOptions? insertManyOptions,
          Options? rawOptions}) async =>
      InsertManyOperationOpen(this, documents,
              session: session,
              insertManyOptions: insertManyOptions?.toManyOpen,
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

    var updateOneOperation = UpdateOneOperationOpen(
        this,
        UpdateOneStatementOpen(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateOneOptions:
            UpdateOneOptions(writeConcern: writeConcern).toUpdateOneOpen,
        rawOptions: rawOptions);
    return updateOneOperation.executeDocument();
  }

  @override
  Future<ReplaceOneDocumentRec> replaceOne(filter, update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      HintUnion? hint,
      ReplaceOneOptions? replaceOneOptions,
      Options? rawOptions}) async {
    QueryUnion uFilter = filter is QueryUnion ? filter : QueryUnion(filter);

    var replaceOneOperation = ReplaceOneOperationOpen(
        this,
        ReplaceOneStatementOpen(uFilter, update,
            upsert: upsert, collation: collation, hint: hint),
        replaceOneOptions:
            ReplaceOneOptions(writeConcern: writeConcern).toReplaceOneOpen,
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

    var updateManyOperation = UpdateManyOperationOpen(
        this,
        UpdateManyStatementOpen(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateManyOptions: updateManyOptions?.toUpdateManyOpen ??
            UpdateManyOptions(writeConcern: writeConcern).toUpdateManyOpen,
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
    var deleteOperation = DeleteOneOperationOpen(
        this, DeleteOneStatementOpen(uFilter, collation: collation, hint: hint),
        session: session,
        deleteOneOptions: deleteOneOptions?.toDeleteOneOpen ??
            DeleteOneOptions(writeConcern: writeConcern).toDeleteOneOpen,
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
    var deleteOperation = DeleteManyOperationOpen(this,
        DeleteManyStatementOpen(uFilter, collation: collation, hint: hint),
        session: session,
        deleteManyOptions: deleteManyOptions?.toDeleteManyOpen ??
            DeleteManyOptions(writeConcern: writeConcern).toDeleteManyOpen,
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

    var famOperation = FindOneAndDeleteOperationOpen(this, uFilter,
        fields: uProjection,
        sort: uSort,
        hint: uHint,
        findOneAndDeleteOptions:
            findOneAndDeleteOptions?.toFindOneAndDeleteOptionsOpen,
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

    var famOperation = FindOneAndReplaceOperationOpen(
        this, uFilter, replacement,
        fields: uProjection,
        sort: uSort,
        returnNew: returnNew,
        upsert: upsert,
        session: session,
        hint: uHint,
        findOneAndReplaceOptions:
            findOneAndReplaceOptions?.toFindOneAndReplaceOptionsOpen,
        rawOptions: rawOptions);
    return famOperation.executeDocument();
  }

  @override
  Future<FindOneAndUpdateDocumentRec> findOneAndUpdate(query, update,
      {dynamic fields,
      sort,
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

    var famOperation = FindOneAndUpdateOperationOpen(this,
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
            findOneAndUpdateOptions?.toFindOneAndUpdateOptionsOpen,
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

    var operation = FindOperationOpen(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        skip: skip,
        limit: 1,
        session: session,
        findOptions: findOptions?.toOpen,
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

    var operation = FindOperationOpen(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        limit: limit,
        skip: skip,
        session: session,
        findOptions: findOptions?.toOpen,
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
        AggregateOperationOpen(pipeline,
            collection: this,
            explain: explain,
            cursor: cursor,
            hint: hint,
            session: session,
            aggregateOptionsOpen: aggregateOptions?.toOpen,
            rawOptions: rawOptions,
            let: let),
        db.server);
  }
}
