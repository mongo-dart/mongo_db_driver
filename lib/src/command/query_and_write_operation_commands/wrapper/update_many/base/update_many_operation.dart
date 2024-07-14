import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../session/session_exp.dart';
import '../../../../base/operation_base.dart';
import '../../../../command_exp.dart';
import '../open/update_many_operation_open.dart';
import '../v1/update_many_operation_v1.dart';

typedef UpdateManyDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument
);

abstract base class UpdateManyOperation extends UpdateOperation {
  @protected
  UpdateManyOperation.protected(
      MongoCollection collection, UpdateManyStatement updateManyStatement,
      {super.ordered,
      super.session,
      UpdateManyOptions? updateManyOptions,
      super.rawOptions})
      : super.protected(collection, [updateManyStatement],
            updateOptions: updateManyOptions);

  factory UpdateManyOperation(
      MongoCollection collection, UpdateManyStatement updateManyStatement,
      {bool? ordered,
      ClientSession? session,
      UpdateManyOptions? updateManyOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return UpdateManyOperationV1(
              collection, updateManyStatement.toUpdateManyV1,
              session: session,
              ordered: ordered,
              updateManyOptions: updateManyOptions?.toUpdateManyV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return UpdateManyOperationOpen(
        collection, updateManyStatement.toUpdateManyOpen,
        ordered: ordered,
        session: session,
        updateManyOptions: updateManyOptions?.toUpdateManyOpen,
        rawOptions: rawOptions);
  }

  Future<MongoDocument> executeUpdateMany() async => process();

  Future<UpdateManyDocumentRec> executeDocument() async {
    var ret = await executeUpdateMany();
    return (WriteResult.fromMap(WriteCommandType.update, ret), ret);
  }
}
