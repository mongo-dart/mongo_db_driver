import 'dart:math';

import 'package:bson/bson.dart';
import 'package:mongo_db_driver/src/core/message/mongo_message.dart';
import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../core/error/mongo_dart_error.dart';
import '../../../../database/base/mongo_collection.dart';
import '../../../../session/client_session.dart';
import '../../../../topology/server.dart';
import '../../../../unions/hint_union.dart';
import '../../../../unions/query_union.dart';
import '../../../../utils/map_keys.dart';
import '../../../command_exp.dart';
import 'bulk_options.dart';

typedef BulkDocumentRec = (
  BulkWriteResult writeResult,
  List<MongoDocument> serverDocument
);

abstract base class Bulk extends CommandOperation {
  Bulk(MongoCollection collection,
      {super.session,
      BulkOptions? bulkOptions,
      Map<String, Object>? rawOptions})
      : super(
            collection.db,
            {},
            <String, dynamic>{
              ...?bulkOptions?.getOptions(collection.db),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.writeOperation);

  var overallInsertDocuments = <Map<String, dynamic>>[];
  var ids = [];
  var operationInputIndex = 0;

  /// Inserts a single document into the collection.
  void insertOne(Map<String, dynamic> document) {
    document[key_id] ??= ObjectId();
    ids.add(document[key_id]);
    overallInsertDocuments.add(document);
    _setCommand(InsertOneOperation(collection!, document));
  }

  /// Inserts nultiple documents into the collection.
  void insertMany(List<Map<String, dynamic>> documents) {
    var documentsNew = <Map<String, dynamic>>[];
    for (var document in documents) {
      document[key_id] ??= ObjectId();
      documentsNew.add(<String, dynamic>{...document});
      ids.add(document[key_id]);
      overallInsertDocuments.add(document);
    }
    _setCommand(InsertManyOperation(collection!, documentsNew));
  }

  /// deleteOne deletes a single document in the collection that match the
  /// filter. If multiple documents match, deleteOne will delete the first
  /// matching document only.
  void deleteOne(DeleteOneStatement deleteRequest) =>
      _setCommand(DeleteOneOperation(collection!, deleteRequest));

  /// Same as deleteOne but in Map format:
  /// Schema:
  /// { deleteOne : {
  ///    "filter" : <Map>,
  ///    "collation": <CollationOptions | Map>,
  ///    "hint": <String | <Map>                 // Available starting in 4.2.1
  ///   }
  /// }
  void deleteOneFromMap(Map<String, Object> docMap, {int? index}) {
    var contentMap = docMap[bulkFilter];
    if (contentMap is! Map<String, Object>) {
      throw MongoDartError('The "$bulkFilter" key of the '
          '"$bulkDeleteOne" element '
          '${index == null ? '' : 'at index $index '}must contain a Map');
    }
    if (docMap[bulkCollation] != null &&
        docMap[bulkCollation] is! CollationOptions &&
        docMap[bulkCollation] is! Map<String, dynamic>) {
      throw MongoDartError('The "$bulkCollation" key of the '
          '"$bulkDeleteOne" element ${index == null ? '' : 'at index $index '}must '
          'contain a CollationOptions element or a Map representation '
          'of a collation');
    }

    deleteOne(DeleteOneStatement(QueryUnion(contentMap),
        collation: docMap[bulkCollation] is Map<String, dynamic>
            ? CollationOptions.fromMap(
                docMap[bulkCollation] as Map<String, Object>)
            : docMap[bulkCollation] as CollationOptions?,
        hint: HintUnion(docMap[bulkHint])));
  }

  /// deleteMany deletes all documents in the collection that match the filter.
  void deleteMany(DeleteManyStatement deleteRequest) =>
      _setCommand(DeleteManyOperation(collection!, deleteRequest));

  /// Same as deleteMany but in Map format:
  /// Schema:
  /// { deleteMany : {
  ///    "filter" : <Map>,
  ///    "collation": <CollationOptions | Map>,
  ///    "hint": <String> | <Map>                // Available starting in 4.2.1
  ///   }
  /// }
  void deleteManyFromMap(Map<String, Object> docMap, {int? index}) {
    var contentMap = docMap[bulkFilter];
    if (contentMap is! Map<String, Object>) {
      throw MongoDartError('The "$bulkFilter" key of the '
          '"$bulkDeleteMany" element '
          '${index == null ? '' : 'at index $index '}must contain a Map');
    }
    if (docMap[bulkCollation] != null &&
        docMap[bulkCollation] is! CollationOptions &&
        docMap[bulkCollation] is! Map<String, dynamic>) {
      throw MongoDartError('The "$bulkCollation" key of the '
          '"$bulkDeleteMany" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a CollationOptions element or a Map representation '
          'of a collation');
    }

    deleteMany(DeleteManyStatement(QueryUnion(contentMap),
        collation: docMap[bulkCollation] is Map
            ? CollationOptions.fromMap(
                docMap[bulkCollation] as Map<String, Object>)
            : docMap[bulkCollation] as CollationOptions?,
        hint: HintUnion(docMap[bulkHint])));
  }

  /// replaceOne replaces a single document in the collection that matches
  /// the filter. If multiple documents match, replaceOne will replace the
  /// first matching document only.
  void replaceOne(ReplaceOneStatement replaceRequest) =>
      _setCommand(ReplaceOneOperation(collection!, replaceRequest));

  /// Same as replaceOne but in Map format.
  /// Schema:
  /// { replaceOne :
  ///    {
  ///       "filter" : <Map>,
  ///       "replacement" : <Map>,
  ///       "upsert" : <bool>,
  ///       "collation": <CollationOptions | Map>,
  ///       "hint": <String> | <Map>                // Available starting in 4.2.1
  ///    }
  /// }
  void replaceOneFromMap(Map<String, Object> docMap, {int? index}) {
    var filterMap = docMap[bulkFilter];
    if (filterMap is! Map<String, Object>) {
      throw MongoDartError('The "$bulkFilter" key of the '
          '"$bulkReplaceOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map');
    }
    if (docMap[bulkReplacement] is! Map<String, Object>) {
      throw MongoDartError('The "$bulkReplacement" key of the '
          '"$bulkReplaceOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map');
    }
    if (docMap[bulkUpsert] != null && docMap[bulkUpsert] is! bool) {
      throw MongoDartError('The "$bulkUpsert" key of the '
          '"$bulkReplaceOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a bool');
    }
    if (docMap[bulkCollation] != null &&
        docMap[bulkCollation] is! CollationOptions &&
        docMap[bulkCollation] is! Map<String, dynamic>) {
      throw MongoDartError('The "$bulkCollation" key of the '
          '"$bulkReplaceOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a CollationOptions element or a Map representation '
          'of a collation');
    }

    replaceOne(ReplaceOneStatement(
        QueryUnion(filterMap), docMap[bulkReplacement] as MongoDocument,
        upsert: docMap[bulkUpsert] as bool?,
        collation: docMap[bulkCollation] is Map<String, dynamic>
            ? CollationOptions.fromMap(
                docMap[bulkCollation] as Map<String, Object>)
            : docMap[bulkCollation] as CollationOptions?,
        hint: HintUnion(docMap[bulkHint])));
  }

  /// updateOne updates a single document in the collection that matches
  /// the filter. If multiple documents match, updateOne will update the
  /// first matching document only.
  void updateOne(UpdateOneStatement updateRequest) =>
      _setCommand(UpdateOneOperation(collection!, updateRequest));

  /// Same as updateOne but in Map format.
  /// Schema:
  /// { updateOne :
  ///    {
  ///       "filter": <Map>,
  ///       "update": <Map or pipeline>,     // Changed in 4.2
  ///       "upsert": <bool>,
  ///       "collation": <CollationOptions | Map>,
  ///       "arrayFilters": [ <filterdocument1>, ... ],
  ///       "hint": <String> | <Map>          // Available starting in 4.2.1
  ///    }
  /// }
  void updateOneFromMap(Map<String, Object> docMap, {int? index}) {
    var filterMap = docMap[bulkFilter];
    if (filterMap is! QueryFilter) {
      throw MongoDartError('The "$bulkFilter" key of the '
          '"$bulkUpdateOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map');
    }
    if (docMap[bulkUpdate] is! Map<String, Object> &&
        docMap[bulkUpdate] is! List<Map<String, dynamic>>) {
      throw MongoDartError('The "$bulkUpdate" key of the '
          '"$bulkUpdateOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map or a pipeline (List<Map>)');
    }
    if (docMap[bulkUpsert] != null && docMap[bulkUpsert] is! bool) {
      throw MongoDartError('The "$bulkUpsert" key of the '
          '"$bulkUpdateOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a bool');
    }
    if (docMap[bulkCollation] != null &&
        docMap[bulkCollation] is! CollationOptions &&
        docMap[bulkCollation] is! Map<String, dynamic>) {
      throw MongoDartError('The "$bulkCollation" key of the '
          '"$bulkUpdateOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a CollationOptions element or a Map representation '
          'of a collation');
    }
    if (docMap[bulkArrayFilters] != null &&
        docMap[bulkArrayFilters] is! List<Map<String, dynamic>>) {
      throw MongoDartError('The "$bulkArrayFilters" key of the '
          '"$bulkUpdateOne" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a List<Map<String, dynamic>> Object');
    }

    updateOne(UpdateOneStatement(QueryUnion(filterMap),
        UpdateUnion(docMap[bulkUpdate] as UpdateDocument),
        upsert: docMap[bulkUpsert] as bool?,
        collation: docMap[bulkCollation] is Map<String, dynamic>
            ? CollationOptions.fromMap(
                docMap[bulkCollation] as Map<String, Object>)
            : docMap[bulkCollation] as CollationOptions?,
        arrayFilters: docMap[bulkArrayFilters] as List?,
        hint: HintUnion(docMap[bulkHint])));
  }

  /// updateMany updates all documents in the collection that match the filter.
  void updateMany(UpdateManyStatement updateRequest) =>
      _setCommand(UpdateManyOperation(collection!, updateRequest));

  /// Same as updateMany but in Map format.
  /// Schema:
  /// { updateMany :
  ///    {
  ///       "filter" : <Map>,
  ///       "update" : <Map or pipeline>,    // Changed in MongoDB 4.2
  ///       "upsert" : <bool>,
  ///       "collation": <CollationOptions | Map>,
  ///       "arrayFilters": [ <filterdocument1>, ... ],
  ///       "hint": <String> | <Map>         // Available starting in 4.2.1
  ///    }
  /// }
  void updateManyFromMap(Map<String, Object> docMap, {int? index}) {
    var filterMap = docMap[bulkFilter];
    if (filterMap is! Map<String, Object>) {
      throw MongoDartError('The "$bulkFilter" key of the '
          '"$bulkUpdateMany" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map');
    }
    if (docMap[bulkUpdate] is! Map<String, Object> &&
        docMap[bulkUpdate] is! List<Map<String, dynamic>>) {
      throw MongoDartError('The "$bulkUpdate" key of the '
          '"$bulkUpdateMany" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a Map or a pipeline (List<Map>)');
    }
    if (docMap[bulkUpsert] != null && docMap[bulkUpsert] is! bool) {
      throw MongoDartError('The "$bulkUpsert" key of the '
          '"$bulkUpdateMany" element at index $index must '
          'contain a bool');
    }
    if (docMap[bulkCollation] != null &&
        docMap[bulkCollation] is! CollationOptions &&
        docMap[bulkCollation] is! Map<String, dynamic>) {
      throw MongoDartError('The "$bulkCollation" key of the '
          '"$bulkUpdateMany" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a CollationOptions element or a Map representation '
          'of a collation');
    }
    if (docMap[bulkArrayFilters] != null &&
        docMap[bulkArrayFilters] is! List<Map<String, dynamic>>) {
      throw MongoDartError('The "$bulkArrayFilters" key of the '
          '"$bulkUpdateMany" element '
          '${index == null ? '' : 'at index $index '}must '
          'contain a List<Map<String, dynamic>> Object');
    }

    updateMany(UpdateManyStatement(QueryUnion(filterMap),
        UpdateUnion(docMap[bulkUpdate] as UpdateDocument),
        upsert: docMap[bulkUpsert] as bool?,
        collation: docMap[bulkCollation] is Map<String, dynamic>
            ? CollationOptions.fromMap(
                docMap[bulkCollation] as Map<String, Object>)
            : docMap[bulkCollation] as CollationOptions?,
        arrayFilters: docMap[bulkArrayFilters] as List?,
        hint: HintUnion(docMap[bulkHint])));
  }

  void _setCommand(CommandOperation operation) =>
      addCommand(operation.$buildCommand());

  void addCommand(Command command);

  List<Command> getBulkCommands();

  List<Map<int, int>> getBulkInputOrigins();

  @override
  Command $buildCommand() =>
      throw StateError('Call getBulkCommands() for bulk operations');

  Future<List<MongoDocument>> executeBulk(Server server,
      {ClientSession? session}) async {
    var retList = <Map<String, dynamic>>[];
    var isOrdered = options[keyOrdered] as bool? ?? true;

    //final options = Map.from(this.options);

    // Todo implement topology
    // Did the user destroy the topology
    /*if (db?.serverConfig?.isDestroyed() ?? false) {
      return callback(MongoDartError('topology was destroyed'));
    }*/

    var commands = getBulkCommands();
    var origins = getBulkInputOrigins();
    var saveOptions = Map<String, dynamic>.from(options);

    var batchIndex = 0;
    for (var command in commands) {
      processOptions(command);
      command.addAll(options);

      if (readPreference != null) {
        // search for the right connection
      }

      var ret = await server.executeCommand(command, this);

      ret[keyCommandType] = command.keys.first;
      if (ret.containsKey(keyWriteErrors)) {
        var writeErrors = ret[keyWriteErrors] as List?;
        for (Map error in writeErrors ?? []) {
          var selectedKey = 0;
          var origin = origins[batchIndex];
          for (var key in origin.keys /* ?? <int>[] */) {
            if (key <= error[keyIndex] && key > selectedKey) {
              selectedKey = key;
            }
          }
          var opInputIndex = origins[batchIndex][selectedKey];
          error[keyOperationInputIndex] = opInputIndex;
        }
      }
      ret[keyBatchIndex] = batchIndex++;

      retList.add(ret);
      if (isOrdered) {
        if (ret[keyOk] == 0.0 ||
                ret.containsKey(
                    keyWriteErrors) /* ||
            ret.containsKey(keyWriteConcernError) */
            ) {
          return retList;
        }
      }

      options = Map<String, Object>.from(saveOptions);
    }
    return retList;
  }

  Future<BulkDocumentRec> executeDocument(Server server,
      {ClientSession? session}) async {
    var executionRetList = await executeBulk(server, session: session);
    BulkWriteResult? ret;
    WriteCommandType writeCommandType;

    for (var executionMap in executionRetList) {
      switch (executionMap['commandType']) {
        case keyInsert:
          writeCommandType = WriteCommandType.insert;
          break;
        case keyUpdate:
          writeCommandType = WriteCommandType.update;
          break;
        case keyDelete:
          writeCommandType = WriteCommandType.delete;
          break;
        default:
          throw StateError('Unknown command type');
      }
      if (ret == null) {
        ret = BulkWriteResult.fromMap(writeCommandType, executionMap);
      } else {
        ret.mergeFromMap(writeCommandType, executionMap);
      }
    }
    if (ret == null) {
      throw MongoDartError('No response from the server');
    }
    //ret.ids = ids.sublist(0, min<int>(ids.length, ret.nInserted));
    return (ret, executionRetList);
  }

  List<Map<int, int>> splitInputOrigins(
      Map<int, int> origins, int commandsLength) {
    if (origins.isEmpty) {
      return [origins];
    }
    var maxWriteBatchSize = MongoMessage.maxWriteBatchSize;
    if (commandsLength <= maxWriteBatchSize) {
      return [origins];
    }
    var ret = <Map<int, int>>[];
    var offset = 0;
    var elementLimit = maxWriteBatchSize - 1;
    var rest = commandsLength;
    Map<int, int> splittedElement;
    var highestKey = 0;
    var highestOperation = 0;
    while (rest > 0) {
      splittedElement = <int, int>{if (offset > 0) 0: highestOperation};
      for (var key in origins.keys) {
        if (key >= offset && key <= elementLimit) {
          if (key > highestKey) {
            highestKey = key;
            highestOperation = origins[key]!;
          }
          splittedElement[key - offset] = origins[key]!;
        }
      }
      offset = elementLimit + 1;
      elementLimit = min(commandsLength, elementLimit + maxWriteBatchSize);
      rest -= maxWriteBatchSize;
      ret.add(splittedElement);
    }

    return ret;
  }

  /// Split the command if the number of documents exceed the maxWriteBatchSixe
  ///
  /// Here we assume that the command is made this way:
  /// { <commandType>: <collectionName>, <commandArgument> : <documentsList>,
  /// ...maybe others}
  List<Command> splitCommands(Command command) {
    var ret = <Command>[];
    if (command.isEmpty) {
      return ret;
    }
    var maxWriteBatchSize = MongoMessage.maxWriteBatchSize;
    var documentsNum = (command.values.toList()[1] as List).length;
    if (documentsNum <= maxWriteBatchSize) {
      ret.add(<String, dynamic>{...command});
    } else {
      var documents = command.values.toList()[1] as List;
      var offset = 0;
      var endSubList = maxWriteBatchSize;
      var rest = documentsNum;
      Map<String, Object> splittedDocument;
      while (rest > 0) {
        splittedDocument = Map.from(command);
        splittedDocument[command.keys.last] =
            documents.sublist(offset, endSubList);
        ret.add(<String, dynamic>{...splittedDocument});
        rest = documentsNum - endSubList;
        offset = endSubList;
        endSubList += min(rest, maxWriteBatchSize);
      }
    }
    return ret;
  }
}
