import 'package:meta/meta.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';

import '../open/replace_one_options_open.dart';
import '../v1/replace_one_options_v1.dart';

abstract class ReplaceOneOptions extends UpdateOptions {
  @protected
  ReplaceOneOptions.protected(
      {super.writeConcern, super.bypassDocumentValidation, super.comment})
      : super.protected();

  factory ReplaceOneOptions(
      {ServerApi? serverApi,
      WriteConcern? writeConcern,
      bool? bypassDocumentValidation = false,
      String? comment}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return ReplaceOneOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);
    }
    return ReplaceOneOptionsOpen(
        writeConcern: writeConcern,
        bypassDocumentValidation: bypassDocumentValidation,
        comment: comment);
  }

  ReplaceOneOptionsOpen get toReplaceOneOpen => this is ReplaceOneOptionsOpen
      ? this as ReplaceOneOptionsOpen
      : ReplaceOneOptionsOpen(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);

  ReplaceOneOptionsV1 get toReplaceOneV1 => this is ReplaceOneOptionsV1
      ? this as ReplaceOneOptionsV1
      : ReplaceOneOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);
}
