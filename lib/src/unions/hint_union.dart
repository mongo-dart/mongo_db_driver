import 'package:mongo_db_driver/mongo_db_driver.dart';

import 'base/union_type.dart';

class HintUnion extends UnionType<String, IndexDocument> {
  HintUnion(super.value);
}
