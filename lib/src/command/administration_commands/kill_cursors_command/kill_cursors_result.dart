import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/src/command/mixin/basic_result.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

class KillCursorsResult with BasicResult {
  KillCursorsResult(Map<String, dynamic> document) {
    extractBasic(document);
    var docs = document[keyCursorsKilled] as List?;
    if (docs != null && docs.isNotEmpty) {
      cursorsKilled = <Int64>[];
      for (var cursorKilled in docs) {
        cursorsKilled!.add(cursorKilled);
      }
    }

    docs = document[keyCursorsNotFound] as List?;
    if (docs != null && docs.isNotEmpty) {
      cursorsNotFound = <Int64>[];
      for (var cursorNotFound in docs) {
        cursorsNotFound!.add(cursorNotFound);
      }
    }

    docs = document[keyCursorsAlive] as List?;
    if (docs != null && docs.isNotEmpty) {
      cursorsAlive = <Int64>[];
      for (var cursorAlive in docs) {
        cursorsAlive!.add(cursorAlive);
      }
    }

    docs = document[keyCursorsUnknown] as List?;
    if (docs != null && docs.isNotEmpty) {
      cursorsUnknown = <Int64>[];
      for (var cursorUnknown in docs) {
        cursorsUnknown!.add(cursorUnknown);
      }
    }
  }

  List<Int64>? cursorsKilled;
  List<Int64>? cursorsNotFound;
  List<Int64>? cursorsAlive;
  List<Int64>? cursorsUnknown;
}
