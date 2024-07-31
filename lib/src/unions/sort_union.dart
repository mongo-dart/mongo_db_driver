import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:type_utils/union.dart';

class SortUnion extends UnionType<IndexDocument, SortExpression> {
  SortUnion(value) : super(transformValue(value));

  static dynamic transformValue(value) {
    if (value is Map<String, dynamic>) {
      IndexDocument sortMap = <String, Object>{...value};
      return sortMap;
    }
    return value;
  }

  IndexDocument get sort {
    if (isNull) {
      return emptyIndexDocument;
    }
    if (valueOne != null) {
      return {...?valueOne};
    }

    return valueTwo!.build();
  }
}
