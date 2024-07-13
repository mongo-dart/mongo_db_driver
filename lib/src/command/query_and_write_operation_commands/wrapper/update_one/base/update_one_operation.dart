import 'package:meta/meta.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart'
    show
        MongoCollection,
        MongoDartError,
        ServerApiVersion,
        UpdateOneOptions,
        UpdateOneStatement,
        UpdateOperation,
        WriteCommandType,
        WriteResult;
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../session/client_session.dart';
import '../../../../base/operation_base.dart';
import '../open/update_one_operation_open.dart';
import '../v1/update_one_operation_v1.dart';

typedef UpdateOneDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument
);

abstract base class UpdateOneOperation extends UpdateOperation {
  @protected
  UpdateOneOperation.protected(
      MongoCollection collection, UpdateOneStatement updateOneStatement,
      {super.session, UpdateOneOptions? updateOneOptions, super.rawOptions})
      : super.protected(collection, [updateOneStatement],
            ordered: false, updateOptions: updateOneOptions);

  factory UpdateOneOperation(
      MongoCollection collection, UpdateOneStatement updateOneStatement,
      {ClientSession? session,
      UpdateOneOptions? updateOneOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return UpdateOneOperationV1(
              collection, updateOneStatement.toUpdateOneV1,
              session: session,
              updateOneOptions: updateOneOptions?.toUpdateOneV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return UpdateOneOperationOpen(
        collection, updateOneStatement.toUpdateOneOpen,
        session: session,
        updateOneOptions: updateOneOptions?.toUpdateOneOpen,
        rawOptions: rawOptions);
  }

  /*  Future<WriteResult> executeDocument() async =>
      WriteResult.fromMap(WriteCommandType.update, await process()); */
  Future<MongoDocument> executeUpdateOne() async => process();

  Future<UpdateOneDocumentRec> executeDocument() async {
    var ret = await executeUpdateOne();
    return (WriteResult.fromMap(WriteCommandType.update, ret), ret);
  }
}
