import 'package:bson/bson.dart';
import 'package:fixnum/fixnum.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

// TODO create test and change hello scan
class TopologyVersion implements Comparable {
  TopologyVersion(this.processId, this.counter);
  TopologyVersion.document(Map<String, dynamic> document)
      : this(document['processId'], document['counter']);

  final ObjectId processId;
  final Int64 counter;

  MongoDocument toMap() => {'processId': processId.oid, 'counter': '$counter'};

  /// Compares two topology versions.
  ///
  /// 1. If the response topologyVersion is unset or the ServerDescription's
  ///    topologyVersion is null, the client MUST assume the response is more recent.
  /// 1. If the response's topologyVersion.processId is not equal to the
  ///    ServerDescription's, the client MUST assume the response is more recent.
  /// 1. If the response's topologyVersion.processId is equal to the
  ///    ServerDescription's, the client MUST use the counter field to determine
  ///    which topologyVersion is more recent.
  ///
  /// ```dart
  /// currentTv <   newTv == -1
  /// currentTv == newTv == 0
  /// currentTv >   newTv == 1
  /// ```
  @override
  int compareTo(other) {
    if (other == null) {
      return -1;
    }
    if (other is! TopologyVersion) {
      return 1;
    }

    if (processId != other.processId) {
      return -1;
    }

    return counter.compareTo(other.counter);
  }
}
