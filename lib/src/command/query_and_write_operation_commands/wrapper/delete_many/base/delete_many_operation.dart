import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/database_exp.dart';
import '../../../../command_exp.dart';
import '/src/command/query_and_write_operation_commands/delete_operation/base/delete_operation.dart';

import '../../../../../session/client_session.dart';
import '../../../../base/operation_base.dart';
import '../open/delete_many_operation_open.dart';
import '../v1/delete_many_operation_v1.dart';

typedef DeleteManyDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument
);

abstract base class DeleteManyOperation extends DeleteOperation {
  @protected
  DeleteManyOperation.protected(
      MongoCollection collection, DeleteManyStatement deleteRequest,
      {super.session,
      DeleteManyOptions? deleteManyOptions,
      Options? rawOptions})
      : super.protected(
          collection,
          [deleteRequest],
          deleteOptions: deleteManyOptions,
          rawOptions: rawOptions,
        );

  factory DeleteManyOperation(
      MongoCollection collection, DeleteManyStatement deleteManyStatement,
      {ClientSession? session,
      DeleteManyOptions? deleteManyOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return DeleteManyOperationV1(
              collection, deleteManyStatement.toDeleteManyV1,
              session: session,
              deleteManyOptions: deleteManyOptions?.toDeleteManyV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return DeleteManyOperationOpen(
        collection, deleteManyStatement.toDeleteManyOpen,
        session: session,
        deleteManyOptions: deleteManyOptions?.toDeleteManyOpen,
        rawOptions: rawOptions);
  }
  Future<MongoDocument> executeDeleteMany() async => process();

  Future<DeleteManyDocumentRec> executeDocument() async {
    var ret = await executeDeleteMany();
    return (WriteResult.fromMap(WriteCommandType.delete, ret), ret);
  }
}
