import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/delete_operation/base/delete_statement.dart';

import '../../../../../database/database_exp.dart';
import '../../../../../unions/unions_exp.dart';
import '../../../../command_exp.dart';
import '../open/delete_many_statement_open.dart';
import '../v1/delete_many_statement_v1.dart';

abstract class DeleteManyStatement extends DeleteStatement {
  @protected
  DeleteManyStatement.protected(super.filter, {super.collation, super.hint})
      : super.protected(limit: 0);

  factory DeleteManyStatement(QueryUnion filter,
      {ServerApi? serverApi, CollationOptions? collation, HintUnion? hint}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return DeleteManyStatementV1(filter, collation: collation, hint: hint);
    }
    return DeleteManyStatementOpen(filter, collation: collation, hint: hint);
  }

  DeleteManyStatementOpen get toDeleteManyOpen =>
      this is DeleteManyStatementOpen
          ? this as DeleteManyStatementOpen
          : DeleteManyStatementOpen(QueryUnion(filter),
              collation: collation, hint: hint);

  DeleteManyStatementV1 get toDeleteManyV1 => this is DeleteManyStatementV1
      ? this as DeleteManyStatementV1
      : DeleteManyStatementV1(QueryUnion(filter),
          collation: collation, hint: hint);
}
