import 'package:meta/meta.dart';
import 'package:mongo_db_driver/src/unions/hint_union.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/server_api.dart';
import '../../../../../database/server_api_version.dart';
import '../../../../../unions/query_union.dart';
import '../../../../command_exp.dart';

abstract class ReplaceOneStatement extends UpdateStatement {
  @protected
  ReplaceOneStatement.protected(QueryUnion q, MongoDocument u,
      {super.upsert, super.collation, super.hint})
      : super.protected(q, UpdateUnion(u), multi: false) {
    if (!UpdateUnion(u).specs.isPureDocument) {
      throw MongoDartError('Invalid document in ReplaceOneStatement. '
          'The document is either null or contains update operators');
    }
  }

  factory ReplaceOneStatement(QueryUnion q, MongoDocument u,
      {ServerApi? serverApi,
      bool? upsert,
      CollationOptions? collation,
      HintUnion? hint}) {
    if (serverApi != null && serverApi.version == ServerApiVersion.v1) {
      return ReplaceOneStatementV1(q, u,
          upsert: upsert, collation: collation, hint: hint);
    }
    return ReplaceOneStatementOpen(q, u,
        upsert: upsert, collation: collation, hint: hint);
  }

  ReplaceOneStatementOpen get toReplaceOneOpen =>
      this is ReplaceOneStatementOpen
          ? this as ReplaceOneStatementOpen
          : ReplaceOneStatementOpen(QueryUnion(q), u.value,
              upsert: upsert, collation: collation, hint: hint);

  ReplaceOneStatementV1 get toReplaceOneV1 => this is ReplaceOneStatementV1
      ? this as ReplaceOneStatementV1
      : ReplaceOneStatementV1(QueryUnion(q), u.value,
          upsert: upsert, collation: collation, hint: hint);
}
