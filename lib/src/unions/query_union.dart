import 'package:mongo_db_query/mongo_db_query.dart';

import 'base/union_type.dart';

class QueryUnion extends MultiUnionType<QueryFilter, FilterExpression,
    QueryExpression, Never, Never> {
  QueryUnion(super.value);

  QueryFilter get query {
    if (isNull) {
      return emptyQueryFilter;
    }
    if (valueOne != null) {
      return {...?valueOne};
    }
    if (valueTwo != null) {
      return valueTwo!.build();
    }
    return valueThree!.rawFilter;
  }
}
