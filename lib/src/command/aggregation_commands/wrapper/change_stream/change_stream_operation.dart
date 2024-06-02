import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../aggregate/aggregate_operation.dart';
import 'change_stream_options.dart';

base class ChangeStreamOperation extends AggregateOperation {
  ChangeStreamOperation(super.pipeline,
      {super.collection,
      super.db,
      int? batchSize,
      super.hint,
      ChangeStreamOptions? changeStreamOptions,
      super.rawOptions})
      : super(
          cursor: batchSize == null
              ? null
              : <String, dynamic>{keyBatchSize: batchSize},
          aggregateOptions: changeStreamOptions,
        ) {
    var doc = <String, dynamic>{aggregateChangeStream: <String, dynamic>{}};
    print('${doc.runtimeType}, $doc');
    print(pipeline.runtimeType);
    pipeline.insert(0, <String, dynamic>{
      if (changeStreamOptions == null)
        aggregateChangeStream: <String, dynamic>{}
      else
        aggregateChangeStream: changeStreamOptions.changeStreamSpecificOptions()
    });
  }
}
