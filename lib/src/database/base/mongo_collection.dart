import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart'
    hide
        QueryFilter,
        ProjectionDocument,
        MongoDocument,
        ArrayFilter,
        UpdateDocument;
import '../../command/base/operation_base.dart';
import '../../unions/hint_union.dart';
import '../../command/aggregation_commands/return_classes/change_event.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_delete/base/find_one_and_delete_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_replace/base/find_one_and_replace_options.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_operation.dart';
import '../../command/query_and_write_operation_commands/wrapper/find_one_and_update/base/find_one_and_update_options.dart';
import '../../session/client_session.dart';
import '../../unions/projection_union.dart';
import '../../unions/sort_union.dart';
import '../../utils/parms_utils.dart';
import '../cursor.dart';

abstract class MongoCollection {
  @protected
  MongoCollection.protected(this.db, this.collectionName);

  factory MongoCollection(MongoDatabase db, String collectionName) {
    // Todo if the serverApi will be available also by collection
    //      receive tha appropriate parameter ad use it instead of
    //      the one from the db class
    if (db.serverApi != null) {
      switch (db.serverApi!.version) {
        case ServerApiVersion.v1:
          return MongoCollectionV1(db, collectionName);
        default:
          throw MongoDartError(
              'Stable Api ${db.serverApi!.version} not managed');
      }
    }
    return MongoCollectionOpen(db, collectionName);
  }

  MongoDatabase db;
  String collectionName;
  ReadPreference? readPreference;

  String fullName() => '${db.databaseName}.$collectionName';

  /// Sets the readPreference at Collection level
  void setReadPref(ReadPreference? readPreference) =>
      this.readPreference = readPreference;

  /// At present it can be defined only at client level
  ServerApi? get serverApi => db.serverApi;

  /// returns true if a Strict Stable Api is required
  bool get isStrict => serverApi?.strict ?? false;

  /// Insert one document into this collection
  /// Returns a WriteResult object
  Future<InsertOneDocumentRec> insertOne(MongoDocument document,
      {ClientSession? session,
      InsertOneOptions? insertOneOptions,
      Options? rawOptions});

  /// Insert many document into this collection
  /// Returns a BulkWriteResult object
  Future<InsertManyDocumentRec> insertMany(List<MongoDocument> documents,
      {ClientSession? session,
      InsertManyOptions? insertManyOptions,
      Options? rawOptions});

  /// Update one document into this collection
  Future<UpdateOneDocumentRec> updateOne(filter, update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      List<dynamic>? arrayFilters,
      HintUnion? hint,
      UpdateOneOptions? updateOneOptions,
      Options? rawOptions});

  /// Replace one document into this collection
  Future<ReplaceOneDocumentRec> replaceOne(filter, MongoDocument update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      HintUnion? hint,
      ClientSession? session,
      ReplaceOneOptions? replaceOneOptions,
      Options? rawOptions});

  /// Updates many documents into this collection
  Future<UpdateManyDocumentRec> updateMany(selector, update,
      {bool? upsert,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      UpdateManyOptions? updateManyOptions,
      Options? rawOptions,
      List<dynamic>? arrayFilters,
      HintUnion? hint});

  /// Deletes one document into this collection
  Future<DeleteOneDocumentRec> deleteOne(selector,
      {WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      DeleteOneOptions? deleteOneOptions,
      Options? rawOptions,
      HintUnion? hint});

  /// Deletes many documents into this collection
  Future<DeleteManyDocumentRec> deleteMany(
      {selector,
      WriteConcern? writeConcern,
      CollationOptions? collation,
      ClientSession? session,
      DeleteManyOptions? deleteManyOptions,
      Options? rawOptions,
      HintUnion? hint});

  Future<FindOneAndDeleteDocumentRec> findOneAndDelete(query,
      {ProjectionDocument? fields,
      sort,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndDeleteOptions? findOneAndDeleteOptions,
      Options? rawOptions});

  Future<FindOneAndReplaceDocumentRec> findOneAndReplace(
      query, MongoDocument replacement,
      {ProjectionDocument? fields,
      sort,
      bool? upsert,
      bool? returnNew,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndReplaceOptions? findOneAndReplaceOptions,
      Options? rawOptions});

  Future<FindOneAndUpdateDocumentRec> findOneAndUpdate(query, update,
      {ProjectionDocument? fields,
      sort,
      bool? upsert,
      bool? returnNew,
      List<ArrayFilter>? arrayFilters,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndUpdateOptions? findOneAndUpdateOptions,
      Options? rawOptions});

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
  Future<Map<String, dynamic>?> findOne(
      {dynamic filter,
      dynamic projection,
      dynamic sort,
      int? skip,
      ClientSession? session,
      dynamic hint,
      FindOptions? findOptions,
      Options? rawOptions});

  /// Behaves like the find method, but allows to define a global
  /// query object containing all the specifications for the query
  // Stream<Map<String, dynamic>> findQuery([QueryExpression? query]) {
  //   return find(query?.filter,
  //       projection: query?.fields,
  //       sort: query?.sortExp,
  //       skip: query?.getSkip(),
  //       limit: query?.getLimit());
  // }

  /// Selects documents in a collection or view and returns a stream
  /// of the selected documents.
  Stream<Map<String, dynamic>> find(
      {dynamic filter,
      dynamic projection,
      dynamic sort,
      int? skip,
      int? limit,
      ClientSession? session,
      dynamic hint,
      FindOptions? findOptions,
      Options? rawOptions});

  @protected
  ProjectionUnion? detectProjectUnion(
      projection, QueryExpression? queryExpression) {
    ProjectionUnion? uProjection;
    if (projection != null) {
      if (projection is ProjectionUnion) {
        uProjection = projection;
      } else {
        uProjection = ProjectionUnion(projection);
      }
    } else {
      if (queryExpression?.fields != null) {
        uProjection = ProjectionUnion(queryExpression!.fields.build());
      }
    }
    return uProjection;
  }

  @protected
  SortUnion? detectSortUnion(sort, QueryExpression? queryExpression) {
    SortUnion? uSort;
    if (sort != null) {
      if (sort is SortUnion) {
        uSort = sort;
      } else {
        uSort = SortUnion(sort);
      }
    } else {
      if (queryExpression?.fields != null) {
        uSort = SortUnion(queryExpression!.sortExp.build());
      }
    }
    return uSort;
  }

  // ****************************************************
  // ***********     AGGREGATE    ***********************
  // ****************************************************

  /// This method returns a stream that can be read or transformed into
  /// a list with `.toList()`
  ///
  /// The pipeline can be either an `AggregationPipelineBuilder` or a
  /// List of Maps (`List<Map<String, Object>>`)
  Stream<Map<String, dynamic>> aggregateToStream(dynamic pipeline,
          {bool? explain,
          Map<String, Object>? cursor,
          HintUnion? hint,
          ClientSession? session,
          AggregateOptions? aggregateOptions,
          Map<String, Object>? rawOptions,
          MongoDocument? let}) =>
      aggregate(pipeline,
              explain: explain,
              cursor: cursor,
              hint: hint,
              session: session,
              aggregateOptions: aggregateOptions,
              rawOptions: rawOptions,
              let: let)
          .stream;

  /// This method returns a cursor that can be read or transformed into
  /// a stream with `stream` (for a stream you can directly call
  /// `aggregateToStream`)
  ///
  /// The pipeline can be either an `AggregationPipelineBuilder` or a
  /// List of Maps (`List<Map<String, Object>>`)
  Cursor aggregate(dynamic pipeline,
      {bool? explain,
      Map<String, Object>? cursor,
      HintUnion? hint,
      ClientSession? session,
      AggregateOptions? aggregateOptions,
      Map<String, Object>? rawOptions,
      MongoDocument? let});

  // **************************************************
  //              Drop Collection
  // **************************************************

  Future<bool> drop(
          {ClientSession? session,
          DropOptions? dropOptions,
          Map<String, Object>? rawOptions}) =>
      db.drop(collectionName,
          session: session, dropOptions: dropOptions, rawOptions: rawOptions);

  // **************************************************
  //                   Count
  // **************************************************

  Future<CountResult> count(
      {QueryExpression? selector,
      Map<String, dynamic>? filter,
      int? limit,
      int? skip,
      CollationOptions? collation,
      HintUnion? hint,
      CountOptions? countOptions,
      Map<String, Object>? rawOptions}) async {
    var countOperation = CountOperation(this,
        query: filter ?? selector?.rawFilter,
        skip: skip,
        limit: limit,
        hint: hint,
        countOptions: countOptions,
        rawOptions: rawOptions);
    return countOperation.executeDocument(db.server);
  }

  // **************************************************
  //                     Watch
  // **************************************************

  Stream<ChangeEvent> watch(Object pipeline,
          {int? batchSize,
          HintUnion? hint,
          ChangeStreamOptions? changeStreamOptions,
          Map<String, Object>? rawOptions}) =>
      watchCursor(pipeline,
              batchSize: batchSize,
              hint: hint,
              changeStreamOptions: changeStreamOptions,
              rawOptions: rawOptions)
          .changeStream;

  Cursor watchCursor(Object pipeline,
          {int? batchSize,
          HintUnion? hint,
          ChangeStreamOptions? changeStreamOptions,
          Map<String, Object>? rawOptions}) =>
      Cursor(
          ChangeStreamOperation(pipeline,
              collection: this,
              hint: hint,
              changeStreamOptions: changeStreamOptions,
              rawOptions: rawOptions),
          db.server);

  // **************************************************
  //                   Distinct
  // **************************************************

  /// Utility method for preparing a DistinctOperation
  DistinctOperation _prepareDistinct(String field,
          {query,
          DistinctOptions? distinctOptions,
          Map<String, Object>? rawOptions}) =>
      DistinctOperation(this, field,
          query: extractfilterMap(query),
          distinctOptions: distinctOptions,
          rawOptions: rawOptions);

  /// Executes a Distinct command on this collection.
  /// Retuns a DistinctResult class.
  Future<DistinctResult> distinct(String field,
          {ClientSession? session,
          query,
          DistinctOptions? distinctOptions,
          Map<String, Object>? rawOptions}) async =>
      _prepareDistinct(field, query: query, distinctOptions: distinctOptions)
          .executeDocument(session: session);

  /// Executes a Distinct command on this collection.
  /// Retuns a Map like received from the server.
  /// Used for compatibility with the legacy method
  Future<Map<String, dynamic>> distinctMap(String field,
          {ClientSession? session,
          query,
          DistinctOptions? distinctOptions,
          Map<String, Object>? rawOptions}) async =>
      _prepareDistinct(field, query: query, distinctOptions: distinctOptions)
          .process();

  /// Analogue of mongodb shell method `db.collection.getIndexes()`
  /// Returns an array that holds a list of documents that identify and describe
  /// the existing indexes on the collection. You must call `getIndexes()`
  ///  on a collection
  Future<List<Map<String, dynamic>>> getIndexes() => listIndexes().toList();

  Map<String, dynamic> _setKeys(String? key, Map<String, dynamic>? keys) {
    if (key != null && keys != null) {
      throw ArgumentError('Only one parameter must be set: key or keys');
    }

    if (key != null) {
      keys = {};
      keys[key] = 1;
    }

    if (keys == null) {
      throw ArgumentError('key or keys parameter must be set');
    }

    return keys;
  }
  // ****************************************************************+
  // ******************** OP_MSG_COMMANDS ****************************
  // *****************************************************************
  // All the following methods are available starting from release 3.6
  // *****************************************************************

  /// This function is provided for all servers starting from version 3.6
  /// For previous releases use the same method on Db class.
  ///
  /// The Reply flag allows the caller to receive the result of
  /// the command without a call to getLastError().
  /// As the format is different from the getLastError() one, for compatibility
  /// reasons, if you specify false, the old format is returned
  /// (but one more getLastError() is performed).
  /// Example of the new format:
  /// {createdCollectionAutomatically: false,
  /// numIndexesBefore: 2,
  /// numIndexesAfter: 3,
  /// ok: 1.0}
  ///
  /// Example of the old format:
  /// {"connectionId" -> 11,
  /// "n" -> 0,
  /// "syncMillis" -> 0,
  /// "writtenTo" -> null,
  /// "err" -> null,
  /// "ok" -> 1.0}
  Future<Map<String, dynamic>> createIndex(
      {ClientSession? session,
      String? key,
      Map<String, dynamic>? keys,
      bool? unique,
      bool? sparse,
      bool? background,
      bool? dropDups,
      Map<String, dynamic>? partialFilterExpression,
      String? name}) async {
    var indexOptions = CreateIndexOptions(this,
        uniqueIndex: unique == true,
        sparseIndex: sparse == true,
        background: background == true,
        dropDuplicatedEntries: dropDups == true,
        partialFilterExpression: partialFilterExpression,
        indexName: name);

    var indexOperation = CreateIndexOperation(
        db, this, _setKeys(key, keys), indexOptions,
        session: session);

    var res = await indexOperation.process();
    if (res[keyOk] == 0.0) {
      // It should be better to create a MongoDartError,
      // but, for compatibility reasons, we throw the received map.
      throw res;
    }

    return res;
  }

  Stream<Map<String, dynamic>> listIndexes(
      {int? batchSize, String? comment, Map<String, Object>? rawOptions}) {
    var indexOptions =
        ListIndexesOptions(batchSize: batchSize, comment: comment);

    var command = ListIndexesCommand(db, this,
        listIndexesOptions: indexOptions, rawOptions: rawOptions);

    return Cursor(command, db.server).stream;
  }

  Future<Map<String, dynamic>> dropIndexes(Object index,
      {ClientSession? session,
      WriteConcern? writeConcern,
      String? comment,
      Map<String, Object>? rawOptions}) {
    var indexOptions =
        DropIndexesOptions(writeConcern: writeConcern, comment: comment);

    var command = DropIndexesCommand(db, this, index,
        dropIndexesOptions: indexOptions,
        rawOptions: rawOptions,
        session: session);

    return command.process();
  }

  Future<BulkDocumentRec> bulkWrite(List<Map<String, Object>> documents,
      {bool ordered = true, WriteConcern? writeConcern}) async {
    Bulk bulk;
    if (ordered) {
      bulk = OrderedBulk(this, writeConcern: writeConcern);
    } else {
      bulk = UnorderedBulk(this, writeConcern: writeConcern);
    }
    var index = -1;
    for (var document in documents) {
      index++;
      if (document.isEmpty) {
        continue;
      }
      var key = document.keys.first;
      var testMap = document[key];
      if (testMap is! Map<String, Object>) {
        throw MongoDartError('The "$key" element at index '
            '$index must contain a Map');
      }
      var docMap = testMap;

      switch (key) {
        case bulkInsertOne:
          if (docMap[bulkDocument] is! Map<String, dynamic>) {
            throw MongoDartError('The "$bulkDocument" key of the '
                '"$bulkInsertOne" element at index $index must '
                'contain a Map');
          }
          bulk.insertOne(docMap[bulkDocument] as Map<String, dynamic>);

          break;
        case bulkInsertMany:
          if (docMap[bulkDocuments] is! List<Map<String, dynamic>>) {
            throw MongoDartError('The "$bulkDocuments" key of the '
                '"$bulkInsertMany" element at index $index must '
                'contain a List of Maps');
          }
          bulk.insertMany(docMap[bulkDocuments] as List<Map<String, dynamic>>);
          break;
        case bulkUpdateOne:
          bulk.updateOneFromMap(docMap, index: index);
          break;
        case bulkUpdateMany:
          bulk.updateManyFromMap(docMap, index: index);
          break;
        case bulkReplaceOne:
          bulk.replaceOneFromMap(docMap, index: index);
          break;
        case bulkDeleteOne:
          bulk.deleteOneFromMap(docMap, index: index);
          break;
        case bulkDeleteMany:
          bulk.deleteManyFromMap(docMap, index: index);
          break;
        default:
          throw StateError('The operation "$key" is not allowed in bulkWrite');
      }
    }

    return bulk.executeDocument(db.server);
  }
}
