import 'package:meta/meta.dart';

import '../../../../core/error/mongo_dart_error.dart';
import '../../../../database/database_exp.dart';
import '../../../../server_api_version.dart';
import '../../../../utils/map_keys.dart';
import '/src/command/base/command_operation.dart';
import '/src/command/base/operation_base.dart';
import '/src/command/query_and_write_operation_commands/delete_operation/base/delete_statement.dart';

import '../../../../session/client_session.dart';
import '../open/delete_operation_open.dart';
import '../v1/delete_operation_v1.dart';
import 'delete_options.dart';

abstract base class DeleteOperation extends CommandOperation {
  @protected
  DeleteOperation.protected(MongoCollection collection, this.deleteRequests,
      {super.session, DeleteOptions? deleteOptions, Options? rawOptions})
      : super(
            collection.db,
            {},
            <String, dynamic>{
              ...?deleteOptions?.getOptions(collection.db),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.writeOperation) {
    if (deleteRequests.isEmpty) {
      throw ArgumentError('Delete request required in delete operation');
    }
  }

  factory DeleteOperation(
      MongoCollection collection, List<DeleteStatement> deleteRequests,
      {ClientSession? session,
      DeleteOptions? deleteOptions,
      Map<String, Object>? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return DeleteOperationV1(collection, deleteRequests,
              session: session,
              deleteOptions: deleteOptions?.toV1,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return DeleteOperationOpen(collection, deleteRequests,
        session: session,
        deleteOptions: deleteOptions?.toOpen,
        rawOptions: rawOptions);
  }

  List<DeleteStatement> deleteRequests;

  @override
  Command $buildCommand() => <String, dynamic>{
        keyDelete: collection!.collectionName,
        keyDeletes: [for (var request in deleteRequests) request.toMap()]
      };
}
