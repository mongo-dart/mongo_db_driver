import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

/// Hello command options;
class HelloOptions {
  final String? comment;

  const HelloOptions({this.comment});

  Options get options => <String, dynamic>{
        if (comment != null) keyComment: comment!,
      };
}
