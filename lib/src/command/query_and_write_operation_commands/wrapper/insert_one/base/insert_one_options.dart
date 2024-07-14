import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/command/parameters/write_concern.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/insert_operation/base/insert_options.dart';
import 'package:mongo_db_driver/src/database/server_api.dart';
import 'package:mongo_db_driver/src/database/server_api_version.dart';

import '../open/insert_one_options_open.dart';
import '../v1/insert_one_options_v1.dart';

/// This class should contain all possible options.
/// Version option are managed through the specialized
/// getters toXXX()
abstract class InsertOneOptions extends InsertOptions {
  @protected
  InsertOneOptions.protected(
      {super.writeConcern, super.bypassDocumentValidation = null})
      : super.protected();

  factory InsertOneOptions(
      {ServerApi? serverApi,
      WriteConcern? writeConcern,
      bool? bypassDocumentValidation = false}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return InsertOneOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation);
    }
    return InsertOneOptionsOpen(
        writeConcern: writeConcern,
        bypassDocumentValidation: bypassDocumentValidation);
  }

  InsertOneOptionsOpen get toOneOpen => this is InsertOneOptionsOpen
      ? this as InsertOneOptionsOpen
      : InsertOneOptionsOpen(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation);

  InsertOneOptionsV1 get toOneV1 => this is InsertOneOptionsV1
      ? this as InsertOneOptionsV1
      : InsertOneOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation);
}
