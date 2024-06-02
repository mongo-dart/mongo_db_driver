import 'package:mongo_db_driver/src/utils/map_keys.dart';
import 'package:mongo_db_driver/src/command/parameters/write_concern.dart';

import '../../../../database/base/mongo_database.dart';
import '../../../base/operation_base.dart';

class BulkOptions {
  /// The WriteConcern for this insert operation
  final WriteConcern? writeConcern;

  /// If true, perform an ordered insert of the documents in the array,
  /// and if an error occurs with one of documents, MongoDB will return without
  /// processing the remaining documents in the array.
  ///
  /// If false, perform an unordered insert, and if an error occurs with one of
  /// documents, continue processing the remaining documents in the array.
  ///
  /// Defaults to true.
  final bool ordered;

  /// Enables insert to bypass document validation during the operation.
  ///  This lets you insert documents that do not meet the validation
  /// requirements.
  ///
  /// **New in version 3.2.**
  final bool bypassDocumentValidation;

  BulkOptions(
      {this.writeConcern, bool? ordered, bool? bypassDocumentValidation})
      : ordered = ordered ?? true,
        bypassDocumentValidation = bypassDocumentValidation ?? false;

  // The db parameter is used to transform the writeConcern into a Map
  /// When a writeConcern is given a Db object must be specified
  Options getOptions(MongoDatabase db) => <String, dynamic>{
        if (writeConcern != null)
          keyWriteConcern: writeConcern!.asMap(db.server.serverStatus),
        if (!ordered) keyOrdered: ordered,
        if (bypassDocumentValidation)
          keyBypassDocumentValidation: bypassDocumentValidation,
      };
}
