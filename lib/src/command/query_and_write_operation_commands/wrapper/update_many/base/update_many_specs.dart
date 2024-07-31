//import 'package:mongo_db_driver/mongo_dart.dart';
//import 'package:mongo_db_driver/mongo_dart_old.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:type_utils/union.dart';

import '../../../../../core/error/mongo_dart_error.dart';

class UpdateManySpecs extends MultiUnionType<UpdateDocument,
    List<UpdateDocument>, UpdateExpression, AggregationPipelineBuilder, Never> {
  UpdateManySpecs(super.value) {
    if (isNull) {
      throw MongoDartError('The update Spec cannpt be null');
    }
  }

  List<UpdateDocument> get specs {
    switch (value.runtimeType) {
      case const (UpdateDocument):
        return <UpdateDocument>[value];
      case const (List<UpdateDocument>):
        return value;
      case const (UpdateExpression):
        return <UpdateDocument>[
          (value as UpdateExpression).build() as UpdateDocument
        ];
      case const (AggregationPipelineBuilder):
        return (value as AggregationPipelineBuilder).build()
            as List<UpdateDocument>;
      default:
        throw MongoDartError(
            'Unexpected value type ${value.runtimeType} in UpdateManySpecs');
    }
  }
}
