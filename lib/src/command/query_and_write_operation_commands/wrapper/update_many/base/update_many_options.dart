import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/database/database_exp.dart';

import '../../../../command_exp.dart';
import '../open/update_many_options_open.dart';
import '../v1/update_many_options_v1.dart';

abstract class UpdateManyOptions extends UpdateOptions {
  @protected
  UpdateManyOptions.protected(
      {super.writeConcern, super.bypassDocumentValidation, super.comment})
      : super.protected();

  factory UpdateManyOptions(
      {ServerApi? serverApi,
      WriteConcern? writeConcern,
      bool? bypassDocumentValidation = false,
      String? comment}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return UpdateManyOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);
    }
    return UpdateManyOptionsOpen(
        writeConcern: writeConcern,
        bypassDocumentValidation: bypassDocumentValidation,
        comment: comment);
  }

  UpdateManyOptionsOpen get toUpdateManyOpen => this is UpdateManyOptionsOpen
      ? this as UpdateManyOptionsOpen
      : UpdateManyOptionsOpen(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);

  UpdateManyOptionsV1 get toUpdateManyV1 => this is UpdateManyOptionsV1
      ? this as UpdateManyOptionsV1
      : UpdateManyOptionsV1(
          writeConcern: writeConcern,
          bypassDocumentValidation: bypassDocumentValidation,
          comment: comment);
}
