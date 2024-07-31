import 'package:mongo_db_driver/src/command/query_and_write_operation_commands/update_operation/base/update_spec.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import 'package:type_utils/union.dart';

import '../../../../core/error/mongo_dart_error.dart';

class UpdateUnion extends HugeUnionType<
    UpdateDocument,
    MongoDocument,
    List<Map<String, dynamic>>,
    UpdateExpression,
    AggregationPipelineBuilder,
    UpdateSpec,
    Never,
    Never,
    Never> {
  UpdateUnion(value) : super(transformValue(value)) {
    if (isNull) {
      print(value.runtimeType);
      throw MongoDartError('The update Union cannpt be null');
    }
  }

  static dynamic transformValue(value) {
    if (value is List) {
      if (value is List<Map<String, dynamic>>) {
        return value;
      }
      List<Map<String, dynamic>> lud = <Map<String, dynamic>>[
        for (var element in value) <String, dynamic>{...?element}
      ];
      return lud;
    }
    return value;
  }

  UpdateSpec get specs {
    if (value is UpdateDocument) {
      return UpdateSpec(valueOne);
    } else if (value is MongoDocument) {
      return UpdateSpec(valueTwo);
    } else if (value is List<Map<String, dynamic>>) {
      return UpdateSpec(valueThree);
    } else if (value is UpdateExpression) {
      return UpdateSpec(valueFour!.build());
    } else if (value is AggregationPipelineBuilder) {
      return UpdateSpec(valueFive!.build());
    } else if (value is UpdateSpec) {
      return value;
    }

    throw MongoDartError(
        'Unexpected value type ${value.runtimeType} in UpdateSpecs');
  }
}
