import 'package:mongo_db_driver/src/command/base/cursor_result.dart';
import 'package:mongo_db_driver/src/command/mixin/basic_result.dart';
import 'package:mongo_db_driver/src/command/mixin/timing_result.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../../session/client_session.dart';

class FindResult with BasicResult, TimingResult {
  FindResult(Map<String, dynamic> document, ClientSession session)
      : cursorResult = CursorResult(
            document[keyCursor] as Map<String, dynamic>? ?? <String, dynamic>{},
            session) {
    extractBasic(document);
    extractTiming(document);
  }
  CursorResult cursorResult;
}
