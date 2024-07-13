import 'package:meta/meta.dart';

import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../server_api_version.dart';
import '../../../../../session/session_exp.dart';
import '../../../../base/operation_base.dart';
import '../../../../command_exp.dart';

typedef InsertManyDocumentRec = (
  BulkWriteResult bulkWriteResult,
  MongoDocument serverDocument,
  List<MongoDocument> insertedDocuments,
  List ids
);

abstract base class InsertManyOperation extends InsertOperation {
  @protected
  InsertManyOperation.protected(
      MongoCollection collection, List<MongoDocument> documents,
      {super.session,
      InsertManyOptions? insertManyOptions,
      Options? rawOptions})
      : super.protected(
          collection,
          documents,
          insertOptions: insertManyOptions,
          rawOptions: rawOptions,
        ) {
    if (documents.isEmpty) {
      throw ArgumentError(
          'At least one document required in InsertManyOperation');
    }
  }

  factory InsertManyOperation(
      MongoCollection collection, List<MongoDocument> documents,
      {ClientSession? session,
      InsertManyOptions? insertManyOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return InsertManyOperationV1(collection, documents,
              session: session,
              insertManyOptions: insertManyOptions?.toManyV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return InsertManyOperationOpen(collection, documents,
        session: session,
        insertManyOptions: insertManyOptions?.toManyOpen,
        rawOptions: rawOptions);
  }

  Future<InsertManyDocumentRec> executeDocument() async {
    var (serverDocument, documents, ids) = await executeInsert();
    return (
      BulkWriteResult.fromMap(WriteCommandType.insert, serverDocument)
      /* ..ids = ids
        ..documents = documents */
      ,
      serverDocument,
      documents,
      ids
    );
  }
}
