import 'package:mongo_db_driver/src/utils/map_keys.dart';

/// Basic communication from a MongoDb server
///
/// returns the following fields:
/// `ok` => 1.0 operation/command successful, 0.0 operation/command failed
/// `errmsg` => Optional - if failed, is the error description
/// `code` => Optional - if failed is the error code
/// `codeName` => Optional - if failed is the error typology
///               Ex. "IndexOptionsConflict"

mixin BasicResult {
  /// Command status (1.0 Ok, 0.0 error)
  double ok = 0.0;

  /// Optional error fields
  String? errmsg;
  int? code;
  String? codeName;

  bool get success => ok == 1.0;
  bool get failure => ok == 0.0;

  void extractBasic(Map<String, dynamic> document) {
    //document ??= <String, dynamic>{};
    var documentOk = document[keyOk];
    if (documentOk is int) {
      ok = documentOk.toDouble();
    } else {
      ok = documentOk as double;
    }
    errmsg = document[keyErrmsg] as String?;
    code = document[keyCode] as int?;
    codeName = document[keyCodeName] as String?;
  }
}
