import 'package:mongo_db_driver/src/utils/map_keys.dart';

import 'abstract_write_result.dart';
import 'write_error.dart';

/// A wrapper that contains the result status of the mongo shell write methods:
/// - insert
/// - update
/// - remove
/// - save
class WriteResult extends AbstractWriteResult {
  //dynamic id;
  //Map<String, dynamic>? document;
  WriteError? writeError;

  WriteResult.fromMap(
      WriteCommandType writeCommandType, Map<String, dynamic> result)
      : super.fromMap(writeCommandType, result) {
    if (result[keyWriteErrors] != null &&
        (result[keyWriteErrors] as List).isNotEmpty) {
      writeError = WriteError.fromMap(
          <String, dynamic>{...?(result[keyWriteErrors] as List).first});
    }
  }

  @override
  bool get hasWriteErrors => writeError != null;

  @override
  int get writeErrorsNumber => hasWriteErrors ? 1 : 0;
}
