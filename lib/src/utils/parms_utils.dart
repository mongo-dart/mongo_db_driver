import 'package:mongo_db_driver/mongo_db_driver.dart' show MongoDartError;
import 'package:mongo_db_query/mongo_db_query.dart';

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
