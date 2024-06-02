import 'dart:async';
import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:logging/logging.dart';

import '../abstract/mongo_response_message.dart';
import '../mongo_message.dart';
import 'packet_converter.dart';

class MessageHandler {
  final _log = Logger('MongoMessageTransformer');
  final converter = PacketConverter();

  void handleData(Uint8List data, EventSink<MongoMessage> sink) {
    converter.addPacket(data);
    while (!converter.messages.isEmpty) {
      var buffer = BsonBinary.from(converter.messages.removeFirst());
      //var opcodeFromWire = MongoResponseMessage.extractOpcode(buffer);
      var reply = MongoMessage.fromBuffer(buffer);

      _log.fine(() => reply.toString());
      sink.add(reply);
    }
  }

  void handleDone(EventSink<MongoResponseMessage> sink) {
    if (!converter.isClear) {
      _log.warning(
          'Invalid state of PacketConverter in handleDone: $converter');
    }
    sink.close();
  }

  StreamTransformer<Uint8List, MongoMessage> get transformer =>
      StreamTransformer<Uint8List, MongoMessage>.fromHandlers(
          handleData: handleData, handleDone: handleDone);
}
