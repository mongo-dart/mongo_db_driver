import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/command/parameters/write_concern.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/insert_operation/base/insert_options.dart';

import '../../../../../server_api.dart';
import '../../../../../server_api_version.dart';
import '../open/insert_many_options_open.dart';
import '../v1/insert_many_options_v1.dart';

class InsertManyOptions extends InsertOptions {
  @protected
  InsertManyOptions.protected(
      {super.writeConcern,
      super.ordered = null,
      super.bypassDocumentValidation = null})
      : super.protected();

  factory InsertManyOptions(
      {ServerApi? serverApi,
      WriteConcern? writeConcern,
      bool? ordered,
      bool? bypassDocumentValidation = false}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return InsertManyOptionsV1(
          writeConcern: writeConcern,
          ordered: ordered,
          bypassDocumentValidation: bypassDocumentValidation);
    }
    return InsertManyOptionsOpen(
        writeConcern: writeConcern,
        ordered: ordered,
        bypassDocumentValidation: bypassDocumentValidation);
  }

  InsertManyOptionsOpen get toManyOpen => this is InsertManyOptionsOpen
      ? this as InsertManyOptionsOpen
      : InsertManyOptionsOpen(
          writeConcern: writeConcern,
          ordered: ordered,
          bypassDocumentValidation: bypassDocumentValidation);

  InsertManyOptionsV1 get toManyV1 => this is InsertManyOptionsV1
      ? this as InsertManyOptionsV1
      : InsertManyOptionsV1(
          writeConcern: writeConcern,
          ordered: ordered,
          bypassDocumentValidation: bypassDocumentValidation);
}
