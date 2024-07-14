import 'package:mongo_db_query/mongo_db_query.dart';

import '../client/client_exp.dart' show MongoDartError;

Map<String, dynamic> extractfilterMap(filter) {
  if (filter == null) {
    return <String, dynamic>{};
  }
  if (filter is QueryExpression) {
    return <String, dynamic>{...filter.rawFilter};
  } else if (filter is Map) {
    return <String, dynamic>{...filter};
  }
  throw MongoDartError(
      'Filter can only be a Map or a QueryExpression instance');
}
