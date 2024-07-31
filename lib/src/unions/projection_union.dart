import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:type_utils/union.dart';

class ProjectionUnion
    extends UnionType<ProjectionDocument, ProjectionExpression> {
  ProjectionUnion(super.value);

  ProjectionDocument get projection {
    if (isNull) {
      return emptyProjectionDocument;
    }
    if (valueOne != null) {
      return {...?valueOne};
    }

    return valueTwo!.build();
  }
}
