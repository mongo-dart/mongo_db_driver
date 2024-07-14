import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/wrapper/replace_one/open/replace_one_operation_open.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../client/client_exp.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../session/session_exp.dart';
import '../../../../base/operation_base.dart';
import '../../../../command_exp.dart';
import '../v1/replace_one_operation_v1.dart';

typedef ReplaceOneDocumentRec = (
  WriteResult writeResult,
  MongoDocument serverDocument
);

abstract base class ReplaceOneOperation extends UpdateOperation {
  @protected
  ReplaceOneOperation.protected(
      MongoCollection collection, ReplaceOneStatement replaceOneStatement,
      {super.session,
      ReplaceOneOptions? replaceOneOptions,
      Options? rawOptions})
      : super.protected(
          collection,
          [replaceOneStatement],
          ordered: false,
          updateOptions: replaceOneOptions,
          rawOptions: rawOptions,
        );

  factory ReplaceOneOperation(
      MongoCollection collection, ReplaceOneStatement replaceOneStatement,
      {ClientSession? session,
      ReplaceOneOptions? replaceOneOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return ReplaceOneOperationV1(
              collection, replaceOneStatement.toReplaceOneV1,
              session: session,
              replaceOneOptions: replaceOneOptions?.toReplaceOneV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return ReplaceOneOperationOpen(
        collection, replaceOneStatement.toReplaceOneOpen,
        session: session,
        replaceOneOptions: replaceOneOptions?.toReplaceOneOpen,
        rawOptions: rawOptions);
  }
  Future<MongoDocument> executeReplaceOne() async => process();

  Future<ReplaceOneDocumentRec> executeDocument() async {
    var ret = await executeReplaceOne();

    return (WriteResult.fromMap(WriteCommandType.update, ret), ret);
  }
}
