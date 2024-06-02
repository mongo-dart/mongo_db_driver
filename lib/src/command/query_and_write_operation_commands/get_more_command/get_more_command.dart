import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import '../../../core/error/mongo_dart_error.dart';
import '../../../database/base/mongo_database.dart';
import '../../../database/base/mongo_collection.dart';
import 'get_more_options.dart';
import 'get_more_result.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

/// getMore command.
///
/// Use in conjunction with commands that return a cursor,
/// e.g. find and aggregate, to return subsequent batches of documents
/// currently pointed to by the cursor.
///
/// The command accepts the following fields:
/// * collection 	[MongoCollection]
///   - The collection over which the cursor is operating.
/// * cursorId int
///   -	The cursor id of the original find or aggregate operations.
/// * getMoreOptions [GetMoreOptions] - Optional
///   - a set of optional values for the command
base class GetMoreCommand extends CommandOperation {
  GetMoreCommand(MongoCollection? collection, Int64 cursorId,
      {MongoDatabase? db,
      String? collectionName,
      super.session,
      GetMoreOptions? getMoreOptions,
      Map<String, Object>? rawOptions})
      : super(
            db ??
                collection?.db ??
                (throw MongoDartError('At least a Db must be specified')),
            <String, dynamic>{
              keyGetMore: cursorId,
              keyCollection: collection?.collectionName ?? collectionName ?? '',
            },
            <String, dynamic>{...?getMoreOptions?.options, ...?rawOptions},
            collection: collection) {
    // In case of aggregate collection agnostic commands, collection is
    // not needed
    if (collection == null) {
      options[keyDbName] = 'admin';
    }
    if (!options.containsKey(keyBatchSize)) {
      options[keyBatchSize] = 101;
    }
  }

  Future<GetMoreResult> executeDocument() async =>
      GetMoreResult(await process(), session);
}
