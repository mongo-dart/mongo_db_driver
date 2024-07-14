import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../session/session_exp.dart';
import '../../../../command_exp.dart';
import '/src/command/query_and_write_operation_commands/delete_operation/base/delete_operation.dart';

import '../../../../base/operation_base.dart';
import '../open/delete_one_operation_open.dart';
import '../v1/delete_one_operation_v1.dart';

typedef DeleteOneDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument
);

abstract base class DeleteOneOperation extends DeleteOperation {
  @protected
  DeleteOneOperation.protected(
      MongoCollection collection, DeleteOneStatement deleteRequest,
      {super.session, DeleteOneOptions? deleteOneOptions, Options? rawOptions})
      : super.protected(
          collection,
          [deleteRequest],
          deleteOptions: deleteOneOptions,
          rawOptions: rawOptions,
        );

  factory DeleteOneOperation(
      MongoCollection collection, DeleteOneStatement deleteOneStatement,
      {ClientSession? session,
      DeleteOneOptions? deleteOneOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return DeleteOneOperationV1(
              collection, deleteOneStatement.toDeleteOneV1,
              session: session,
              deleteOneOptions: deleteOneOptions?.toDeleteOneV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return DeleteOneOperationOpen(
        collection, deleteOneStatement.toDeleteOneOpen,
        session: session,
        deleteOneOptions: deleteOneOptions?.toDeleteOneOpen,
        rawOptions: rawOptions);
  }

  Future<MongoDocument> executeDeleteOne() async => process();

  Future<DeleteOneDocumentRec> executeDocument() async {
    var ret = await executeDeleteOne();
    return (WriteResult.fromMap(WriteCommandType.delete, ret), ret);
  }
}
