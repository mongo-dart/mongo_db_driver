import 'package:meta/meta.dart';

import '../../../../../database/database_exp.dart';
import '../../../../../unions/unions_exp.dart';
import '../../../../command_exp.dart';
import '../open/update_many_statement_open.dart';
import '../v1/update_many_statement_v1.dart';

abstract class UpdateManyStatement extends UpdateStatement {
  @protected
  UpdateManyStatement.protected(super.q, super.u,
      {super.upsert, super.collation, super.arrayFilters, super.hint})
      : super.protected(multi: true);

  factory UpdateManyStatement(QueryUnion q, UpdateUnion u,
      {ServerApi? serverApi,
      bool? upsert,
      CollationOptions? collation,
      List<dynamic>? arrayFilters,
      HintUnion? hint}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return UpdateManyStatementV1(q, u,
          upsert: upsert,
          collation: collation,
          arrayFilters: arrayFilters,
          hint: hint);
    }
    return UpdateManyStatementOpen(q, u,
        upsert: upsert,
        collation: collation,
        arrayFilters: arrayFilters,
        hint: hint);
  }

  UpdateManyStatementOpen get toUpdateManyOpen =>
      this is UpdateManyStatementOpen
          ? this as UpdateManyStatementOpen
          : UpdateManyStatementOpen(QueryUnion(q), UpdateUnion(u.value),
              upsert: upsert,
              collation: collation,
              arrayFilters: arrayFilters,
              hint: hint);

  UpdateManyStatementV1 get toUpdateManyV1 => this is UpdateManyStatementV1
      ? this as UpdateManyStatementV1
      : UpdateManyStatementV1(QueryUnion(q), UpdateUnion(u.value),
          upsert: upsert,
          collation: collation,
          arrayFilters: arrayFilters,
          hint: hint);
}
