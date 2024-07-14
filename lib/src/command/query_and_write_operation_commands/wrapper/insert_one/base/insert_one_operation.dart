import 'package:meta/meta.dart';

import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../client/client_exp.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../session/session_exp.dart';
import '../../../../base/operation_base.dart';
import '../../../../command_exp.dart';

typedef InsertOneRec = (
  MongoDocument serverDocument,
  MongoDocument insertedDocument,
  dynamic id
);
typedef InsertOneDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument,
  MongoDocument insertedDocument,
  dynamic id
);

abstract base class InsertOneOperation extends InsertOperation {
  Map<String, dynamic> document;

  @protected
  InsertOneOperation.protected(MongoCollection collection, this.document,
      {super.session, InsertOneOptions? insertOneOptions, Options? rawOptions})
      : super.protected(
          collection,
          [document],
          insertOptions: insertOneOptions,
          rawOptions: rawOptions,
        );

  factory InsertOneOperation(MongoCollection collection, MongoDocument document,
      {ClientSession? session,
      InsertOneOptions? insertOneOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return InsertOneOperationV1(collection, document,
              session: session,
              insertOneOptions: insertOneOptions?.toOneV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return InsertOneOperationOpen(collection, document,
        session: session,
        insertOneOptions: insertOneOptions?.toOneOpen,
        rawOptions: rawOptions);
  }

  Future<InsertOneRec> executeInsertOne() async {
    var (ret, documents, ids) = await executeInsert();
    return (ret, documents.first, ids.first);
  }

  Future<InsertOneDocumentRec> executeDocument() async {
    var (ret, document, id) = await executeInsertOne();
    return (
      WriteResult.fromMap(WriteCommandType.insert, ret),
      ret,
      document,
      id
    );
  }
}
