import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../core/error/mongo_dart_error.dart';
import '../../client/mongo_client.dart';
import '../../session/client_session.dart';
import '../../topology/abstract/topology.dart';
import '../../topology/server.dart';

enum Aspect {
  readOperation,
  noInheritOptions,
  writeOperation,
  retryable,
}

typedef Options = Map<String, dynamic>;
typedef Command = Map<String, dynamic>;

abstract base class OperationBase {
  Options options;
  final Set<Aspect> _aspects;

  OperationBase(this.mongoClient,
      {Options? options, ClientSession? session, dynamic aspects})
      // Leaves the orginal Options document untouched
      : options = <String, dynamic>{...?options},
        isImplicitSession = session == null,
        _aspects = defineAspects(aspects),
        session = session ?? ClientSession(mongoClient);

  bool isImplicitSession;
  final ClientSession session;

  /// Some commanda, like Hello can be run without first having authenticated
  /// the connection. In this case the default value is overridden in the
  /// derived class.
  bool requiresAuthentication = true;

  bool hasAspect(Aspect aspect) => _aspects.contains(aspect);
  MongoClient mongoClient;
  Topology get topology =>
      mongoClient.topology ??
      (throw MongoDartError('Topology not yet identified'));

  //Object? get session => options[keySession];

  // Todo check if this was the meaning of:
  //   Object.assign(this.options, { session });
  /* set session(Object? value) =>
      value == null ? null : options[keySession] = value; */

  //void clearSession() => options.remove(keySession);

  static Set<Aspect> defineAspects(aspects) {
    if (aspects is Aspect) {
      return {aspects};
    } else if (aspects is List<Aspect>) {
      return {...aspects};
    }
    return {Aspect.noInheritOptions};
  }

  bool get canRetryRead => true;

  /// This method is for internal processing
  @protected
  Future<MongoDocument> process();

  @protected
  Future<MongoDocument> executeOnServer(Server server);
}
