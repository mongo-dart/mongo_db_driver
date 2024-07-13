import 'package:mongo_db_query/mongo_db_query.dart';

import 'base/union_type.dart';

class HintUnion extends UnionType<String, IndexDocument> {
  HintUnion(super.value);
}
