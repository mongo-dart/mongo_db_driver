import 'package:mongo_db_driver/src/utils/map_keys.dart';
import 'package:mongo_db_driver/src/utils/mongo_db_namespace.dart';

class ChangeEvent {
  ChangeEvent.fromMap(Map<String, dynamic> streamData) {
    serverResponse = _extractEventData(streamData);
  }

  late Map<String, dynamic> serverResponse;

  /// Metadata related to the operation. Acts as the resumeToken for the
  /// resumeAfter parameter when resuming a change stream.
  /// `{
  ///   "_data" : -BinData|hex string-
  /// }`
  ///
  /// The _data type depends on the MongoDB versions and, in some cases,
  /// the feature compatibility version (fcv) at the time of the change stream’s
  /// opening/resumption. For details, see [Resume Tokens](https://docs.mongodb.com/manual/changeStreams/#change-stream-resume-token).
  Map<String, dynamic>? id;

  /// The type of operation that occurred. Can be any of the following values:
  /// - insert
  /// - delete
  /// - replace
  /// - update
  /// - drop
  /// - rename
  /// - dropDatabase
  /// - invalidate
  String? operationType;

  /// The document created or modified by the insert, replace, delete,
  /// update operations (i.e. CRUD operations).
  ///
  /// For insert and replace operations, this represents the new document
  /// created by the operation.
  ///
  /// For delete operations, this field is omitted as the document no
  /// longer exists.
  ///
  /// For update operations, this field only appears if you configured the
  /// change stream with fullDocument set to updateLookup.
  /// This field then represents the most current majority-committed version of
  /// the document modified by the update operation.
  /// This document may differ from the changes described in updateDescription
  /// if other majority-committed operations modified the document between
  /// the original update operation and the full document lookup.
  Map<String, dynamic>? fullDocument;

  /// The document key of the document created or modified by the insert,
  /// replace, delete, update operations (i.e. CRUD operations).
  ///
  /// This is useful in case of a delete operation and avoid to parse
  /// serverResponse documentKey object directly.
  Map<String, dynamic>? documentKey;

  /// The namespace (database and or collection) affected by the event.
  MongoDBNamespace? ns;

  bool get isInsert => operationType == 'insert';
  bool get isDelete => operationType == 'delete';
  bool get isReplace => operationType == 'replace';
  bool get isUpdate => operationType == 'update';
  // not yet managed...
  bool get isDrop => operationType == 'drop';
  bool get isRename => operationType == 'rename';
  bool get isDropDatabase => operationType == 'dropDatabase';
  bool get isInvalidate => operationType == 'invalidate';

  Map<String, dynamic> _extractEventData(Map<String, dynamic> streamData) {
    if (streamData[key_id] != null) {
      id = <String, dynamic>{...streamData[key_id] as Map};
    }
    operationType = streamData[keyOperationType] as String?;
    fullDocument = streamData[keyFullDocument] as Map<String, dynamic>?;
    documentKey = streamData[keyDocumentKey] as Map<String, dynamic>?;
    if (streamData[keyNs] != null) {
      ns = MongoDBNamespace.fromMap(
          <String, dynamic>{...streamData[keyNs] as Map});
    }
    return Map.from(streamData);
  }
}
