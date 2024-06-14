import 'package:mongo_db_driver/src/command/parameters/write_concern.dart';

import '../../../database/base/mongo_database.dart';
import '../../../utils/map_keys.dart';
import '../../base/operation_base.dart';

/// Drop command options;
///
/// Optional parameters that can be used whith the drop command:
class DropOptions {
  /// Optional. A document expressing the write concern of the drop command.
  /// Omit to use the default write concern.
  ///
  /// When issued on a sharded cluster, mongos converts the write concern of
  /// the drop command and its helper db.collection.drop() to "majority"
  final WriteConcern? writeConcern;

  /// A user-provided comment to attach to this command. Once set,
  /// this comment appears alongside records of this command in the following
  /// locations:
  /// - mongod log messages, in the attr.command.cursor.comment field.
  /// - Database profiler output, in the command.comment field.
  /// - currentOp output, in the command.comment field.
  ///
  /// Mongo db allows any comment type, but we restrict it to String
  ///
  /// New in version 4.4.
  final String? comment;

  DropOptions({
    this.comment,
    this.writeConcern,
  });

  Options getOptions(MongoDatabase db) => <String, dynamic>{
        if (writeConcern != null)
          keyWriteConcern: writeConcern!.asMap(db.server.serverStatus),
        if (comment != null) keyComment: comment!,
      };
}
