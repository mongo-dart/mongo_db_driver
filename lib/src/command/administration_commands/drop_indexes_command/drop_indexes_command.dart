import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../../core/error/mongo_dart_error.dart';
import '../../../core/network/abstract/connection_base.dart';
import '../../../database/base/mongo_database.dart';
import '../../../database/base/mongo_collection.dart';
import 'drop_indexes_options.dart';

base class DropIndexesCommand extends CommandOperation {
  Object index;
  late Map<String, Object> indexes;

  DropIndexesCommand(MongoDatabase db, MongoCollection collection, this.index,
      {super.session,
      DropIndexesOptions? dropIndexesOptions,
      ConnectionBase? connection,
      Map<String, Object>? rawOptions})
      : super(
            db,
            {},
            <String, dynamic>{
              ...?dropIndexesOptions?.getOptions(collection),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.writeOperation) {
    if (index is! String && index is! List<String> && index is! Map) {
      throw MongoDartError(
          'The index parameter is not String or List of string or an array');
    }
  }

  @override
  Command $buildCommand() => <String, dynamic>{
        keyDropIndexes: collection!.collectionName,
        keyIndex: index
      };
}
