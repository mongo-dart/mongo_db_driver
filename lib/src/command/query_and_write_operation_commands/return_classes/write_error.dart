import 'package:mongo_db_driver/src/utils/map_keys.dart';

/// contains information regarding any error, excluding write concern errors,
/// encountered during the write operation.
class WriteError {
  /// An integer value identifying the error.
  final int? code;

  /// A description of the error.
  final String? errmsg;

  WriteError(this.code, this.errmsg);

  WriteError.fromMap(Map<String, dynamic> writeErrorMap)
      : code = writeErrorMap[keyCode] as int?,
        errmsg = writeErrorMap[keyErrmsg] as String?;
}
