import 'package:mongo_db_driver/src/command/base/cursor_result.dart';
import 'package:mongo_db_driver/src/command/mixin/basic_result.dart';
import 'package:mongo_db_driver/src/command/mixin/timing_result.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../../session/client_session.dart';

class GetMoreResult with BasicResult, TimingResult {
  GetMoreResult(Map<String, dynamic> document, ClientSession session)
      : cursorResult = CursorResult(
            <String, Object>{...?(document[keyCursor] as Map?)}, session) {
    // TODO throw error if document contains error
    extractBasic(document);
    extractTiming(document);
  }

  CursorResult cursorResult;
}
