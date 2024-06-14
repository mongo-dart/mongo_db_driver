import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_driver/src/utils/mongo_db_namespace.dart';

import '../../core/error/mongo_dart_error.dart';
import '../../session/client_session.dart';
import '../../utils/map_keys.dart';

/// Contains the cursor information,
/// including the cursor id and the firstBatch/nextBatch of documents.
///
/// *Note*
/// Starting in 4.4, if the operation against a sharded collection returns
/// partial results due to the unavailability of the queried shard(s),
/// the cursor document includes a partialResultsReturned field.
/// To return partial results, rather than error, due to the unavailability
/// of the queried shard(s), the find command must run with
/// `allowPartialResults` set to `true`. See allowPartialResults.
/// If the queried shards are initially available for the find command
/// but one or more shards become unavailable in subsequent getMore commands,
/// only the getMore commands run when a queried shard or shards are
/// unavailable include the partialResultsReturned flag in the output.
class CursorResult {
  late Int64 id;
  MongoDBNamespace? ns;
  ClientSession session;

  /// alternative container for documents:
  /// * firstBatch in response to a find/aggreagate operation
  /// * nextBatch in response to a getMore command
  late List<Map<String, dynamic>> firstBatch, nextBatch;
  late bool partialResultsReturned;

  CursorResult(Map<String, dynamic> document, this.session) {
    _extract(document);
  }

  void _extract(Map<String, dynamic> document) {
    //document ??= <String, dynamic>{};
    if (document[keyId] == null) {
      throw MongoDartError('Missing cursor Id');
    }
    id = document[keyId] as Int64;
    // on errors "ns is not returned"
    if (document[keyNs] != null) {
      ns = MongoDBNamespace.fromString(document[keyNs] as String);
    }
    firstBatch = [...(document[keyFirstBatch] as List? ?? [])];
    nextBatch = [...(document[keyNextBatch] as List? ?? [])];
    partialResultsReturned =
        document[keyPartialResultsReturned] as bool? ?? false;
  }
}
