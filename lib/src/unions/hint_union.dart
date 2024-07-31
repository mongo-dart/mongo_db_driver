import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:type_utils/union.dart';

class HintUnion extends UnionType<String, IndexDocument> {
  HintUnion(super.value);
}
