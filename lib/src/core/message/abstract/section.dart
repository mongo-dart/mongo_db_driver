import 'package:bson/bson.dart' show BsonBinary;
import 'package:mongo_db_driver/src/core/message/mongo_message.dart'
    show MongoMessage;
import 'package:mongo_db_query/mongo_db_query.dart';
import '../../error/mongo_dart_error.dart';
import 'payload.dart' show Payload, Payload0, Payload1;

abstract class Section {
  int payloadType;
  Payload payload;

  Section._(this.payloadType, this.payload);

  factory Section(int payloadType, MongoDocument data) {
    if (payloadType == MongoMessage.basePayloadType) {
      return SectionType0.fromDocument(payloadType, data);
    } else if (payloadType == MongoMessage.documentsPayloadType) {
      return SectionType1.fromDocument(payloadType, data);
    }
    throw MongoDartError('Unknown Payload Type "$payloadType"');
  }

  factory Section.fromBuffer(BsonBinary buffer) {
    var payloadType = buffer.readByte();
    if (payloadType == MongoMessage.basePayloadType) {
      return SectionType0(payloadType, Payload0.fromBuffer(buffer));
    } else if (payloadType == MongoMessage.documentsPayloadType) {
      return SectionType1(payloadType, Payload1.fromBuffer(buffer));
    }
    throw MongoDartError('Unknown Payload Type "$payloadType"');
  }

  int get byteLength => 1 /* payloadType */ + payload.byteLength;

  void packValue(BsonBinary buffer) {
    buffer.writeByte(payloadType);
    payload.packValue(buffer);
  }
}

class SectionType0 extends Section {
  SectionType0.fromDocument(int payloadType, MongoDocument document)
      : super._(payloadType, Payload0(document));

  SectionType0(super.payloadType, Payload0 super.payload) : super._();
}

class SectionType1 extends Section {
  factory SectionType1.fromDocument(int payloadType, MongoDocument document) {
    if (document.length > 1) {
      throw MongoDartError('Expected only one element in the '
          'document while generating section 1');
    }
    if (document.values.first is! List) {
      throw MongoDartError(
          'The value of the document parameter must be a List of documents');
    }
    var identifier = document.keys.first;
    var documents = document.values.first as List<Map<String, dynamic>>;
    //payload = Payload1(identifier, documents);
    return SectionType1(payloadType, Payload1(identifier, documents));
  }

  SectionType1(super.payloadType, Payload1 super.payload) : super._();
}
