import 'package:bson/bson.dart' show BsonBinary;

import '../../error/mongo_dart_error.dart';
import 'mongo_base_message.dart';

abstract class MongoResponseMessage extends MongoBaseMessage {
  MongoBaseMessage deserialize(BsonBinary buffer) {
    throw MongoDartError('Must be implemented');
  }

  static int extractOpcode(BsonBinary buffer) {
    buffer.readInt32();
    buffer.readInt32();
    buffer.readInt32();
    var opcodeFromWire = buffer.readInt32();
    buffer.offset -= 16;
    return opcodeFromWire;
  }
}
