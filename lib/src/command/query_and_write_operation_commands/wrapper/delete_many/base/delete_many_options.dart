import 'package:meta/meta.dart';

import '../../../../../database/database_exp.dart';
import '../../../../command_exp.dart';
import '../open/delete_many_options_open.dart';
import '../v1/delete_many_options_v1.dart';

abstract class DeleteManyOptions extends DeleteOptions {
  @protected
  DeleteManyOptions.protected(
      {super.writeConcern, super.ordered, super.comment})
      : super.protected();

  factory DeleteManyOptions(
      {ServerApi? serverApi,
      WriteConcern? writeConcern,
      bool? ordered,
      String? comment}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return DeleteManyOptionsV1(
          writeConcern: writeConcern, ordered: ordered, comment: comment);
    }
    return DeleteManyOptionsOpen(
        writeConcern: writeConcern, ordered: ordered, comment: comment);
  }

  DeleteManyOptionsOpen get toDeleteManyOpen => this is DeleteManyOptionsOpen
      ? this as DeleteManyOptionsOpen
      : DeleteManyOptionsOpen(
          writeConcern: writeConcern, ordered: ordered, comment: comment);

  DeleteManyOptionsV1 get toDeleteManyV1 => this is DeleteManyOptionsV1
      ? this as DeleteManyOptionsV1
      : DeleteManyOptionsV1(
          writeConcern: writeConcern, ordered: ordered, comment: comment);
}
