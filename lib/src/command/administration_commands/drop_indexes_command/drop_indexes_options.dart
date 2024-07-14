import '../../../client/client_exp.dart';
import '../../../database/database_exp.dart';
import '../../base/operation_base.dart';
import '../../command_exp.dart' show WriteConcern;

class DropIndexesOptions {
  /// The WriteConcern for this insert operation
  WriteConcern? writeConcern;

  /// A user-provided comment to attach to this command. Once set,
  /// this comment appears alongside records of this command in the following
  /// locations:
  /// - mongod log messages, in the attr.command.cursor.comment field.
  /// - Database profiler output, in the command.comment field.
  /// - currentOp output, in the command.comment field.
  /// We limit Comment to String only
  ///
  /// New in version 4.4.
  final String? comment;

  DropIndexesOptions({this.writeConcern, this.comment});

  Options getOptions(MongoCollection collection) => <String, dynamic>{
        if (writeConcern != null)
          keyWriteConcern:
              writeConcern!.asMap(collection.db.server.serverStatus),
        if (comment != null) keyComment: comment!,
      };
}
