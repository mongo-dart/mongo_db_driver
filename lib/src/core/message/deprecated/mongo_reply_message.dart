import 'package:bson/bson.dart';
// ignore: implementation_imports
import 'package:bson/src/types/bson_map.dart';

import '../abstract/mongo_base_message.dart';
import '../abstract/mongo_response_message.dart';

class MongoReplyMessage extends MongoResponseMessage {
  static const flagsCursorNone = 0;
  static const flagsCursorNotFound = 1;
  static const flagsQueryFailure = 2;
  static const flagsShardConfigStale = 4;
  static const flagsAwaitCapable = 8;

  int? responseFlags;
  int cursorId = -1; // 64bit integer
  int? startingFrom;
  int numberReturned = -1;
  List<Map<String, dynamic>>? documents;

  @override
  MongoBaseMessage deserialize(BsonBinary buffer) {
    readMessageHeaderFrom(buffer);
    responseFlags = buffer.readInt32();
    cursorId = buffer.readInt64();
    startingFrom = buffer.readInt32();
    numberReturned = buffer.readInt32();
    documents = List<Map<String, dynamic>>.filled(
        numberReturned, const <String, dynamic>{});
    for (var n = 0; n < numberReturned; n++) {
      var doc = BsonMap.fromBuffer(buffer);
      documents![n] = doc.value;
    }
    return this;
  }

  @override
  String toString() {
    if (documents?.length == 1) {
      return 'MongoReplyMessage(ResponseTo:$responseTo, cursorId: $cursorId, '
          'numberReturned:$numberReturned, responseFlags:$responseFlags, '
          '${documents!.first})';
    }
    return 'MongoReplyMessage(ResponseTo:$responseTo, cursorId: $cursorId, '
        'numberReturned:$numberReturned, responseFlags:$responseFlags, '
        '$documents)';
  }
}
