import 'package:mongo_db_driver/src/unions/hint_union.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../command/base/operation_base.dart';
import '../../command/command.dart';
import '../../command/query_and_write_operation_commands/update_operation/base/update_union.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_options.dart';
import '../../session/client_session.dart';
import '../../unions/query_union.dart';
import '../database.dart'
    hide MongoDocument, ProjectionDocument, IndexDocument, ArrayFilter;
import '../cursor.dart';

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

    var updateOneOperation = UpdateOneOperation(
        this,
        UpdateOneStatement(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateOneOptions: UpdateOneOptions(writeConcern: writeConcern),
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

    var replaceOneOperation = ReplaceOneOperation(
        this,
        ReplaceOneStatement(uFilter, update,
            upsert: upsert, collation: collation, hint: hint),
        replaceOneOptions: ReplaceOneOptions(writeConcern: writeConcern),
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

    var updateManyOperation = UpdateManyOperation(
        this,
        UpdateManyStatement(uFilter, uUpdate,
            upsert: upsert,
            collation: collation,
            arrayFilters: arrayFilters,
            hint: hint),
        session: session,
        updateManyOptions:
            updateManyOptions ?? UpdateManyOptions(writeConcern: writeConcern),
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
    var deleteOperation = DeleteOneOperation(
        this, DeleteOneStatement(uFilter, collation: collation, hint: hint),
        session: session,
        deleteOneOptions:
            deleteOneOptions ?? DeleteOneOptions(writeConcern: writeConcern),
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
    var deleteOperation = DeleteManyOperation(
        this, DeleteManyStatement(uFilter, collation: collation, hint: hint),
        session: session,
        deleteManyOptions:
            deleteManyOptions ?? DeleteManyOptions(writeConcern: writeConcern),
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

    var famOperation = FindOneAndDeleteOperation(this, uFilter,
        fields: uProjection,
        sort: uSort,
        hint: uHint,
        findOneAndDeleteOptions: findOneAndDeleteOptions,
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

    var famOperation = FindOneAndReplaceOperation(this, uFilter, replacement,
        fields: uProjection,
        sort: uSort,
        returnNew: returnNew,
        upsert: upsert,
        session: session,
        hint: uHint,
        findOneAndReplaceOptions: findOneAndReplaceOptions,
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

    var famOperation = FindOneAndUpdateOperation(this,
        query: uFilter,
        update: UpdateUnion(update),
        fields: uProjection,
        sort: uSort,
        upsert: upsert,
        returnNew: returnNew,
        arrayFilters: arrayFilters,
        session: session,
        hint: uHint,
        findOneAndUpdateOptions: findOneAndUpdateOptions,
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

    var operation = FindOperation(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        skip: skip,
        limit: 1,
        session: session,
        findOptions: findOptions,
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

    var operation = FindOperation(this, uFilter,
        sort: uSort,
        projection: uProjection,
        hint: uHint,
        limit: limit,
        skip: skip,
        session: session,
        findOptions: findOptions,
        rawOptions: rawOptions);

    return Cursor(operation, db.server).stream;
  }
}
