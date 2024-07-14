import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';
import '../base/server_command.dart';

import '../../client/client_exp.dart';
import '../../topology/abstract/topology.dart';
import '../../topology/server.dart';
import '../command_exp.dart';
import 'operation_base.dart';

/// Run a simple command
///
/// Designed for system commands where Db/Collection are not needed
base class SimpleCommand extends ServerCommand {
  SimpleCommand(super.mongoClient, super.command,
      {super.options,
      super.session,
      super.aspect,
      ReadPreference? readPreference})
      : super();

  /// The ReadPreference Object has prefernce with respect to the options
  /// ReadPrefernce Specs
  ReadPreference? readPreference;

  @override
  @nonVirtual
  Future<MongoDocument> process() async {
    Server? server;
    if (topology.type == TopologyType.standalone) {
      ReadPreference.removeReadPreferenceFromOptions(options);
      server = topology.primary;
    } else if (topology.type == TopologyType.replicaSet) {
      server = topology.getServer(
          readPreferenceMode:
              readPreference?.mode ?? ReadPreferenceMode.primary);
    } else if (topology.type == TopologyType.shardedCluster) {
      server = topology.getServer();
      readPreference ??= options[keyReadPreference] == null
          ? null
          : ReadPreference.fromOptions(options, removeFromOriginalMap: true);
      ReadPreference.removeReadPreferenceFromOptions(options);
      if (readPreference != null) {
        options = {...options, ...readPreference!.toMap()};
      }
    }

    return super.executeOnServer(
        server ?? (throw MongoDartError('No server detected')));
  }

  @override
  void processOptions(Command command) {
    // Get the db the command requires
    final dbName = mongoClient.defaultDatabaseName;

    options.removeWhere((key, value) => key == keyDbName);
    command[key$Db] = dbName;

    if (hasAspect(Aspect.writeOperation)) {
      applyWriteConcern(options, options: options);
      readPreference = ReadPreference.primary;
    } else {
      options.remove(keyWriteConcern);
      // if Aspect is noInheritOptions, here a separated method is maintained
      // even if not necessary, waiting for the future check of the session
      // value.
      readPreference = resolveReadPreference(mongoClient,
              options: options,
              inheritReadPreference: !hasAspect(Aspect.noInheritOptions)) ??
          ReadPreference.primary;
    }
    options.remove(keyReadPreference);

    options.removeWhere((key, value) => command.containsKey(key));

    if (mongoClient.serverApi != null) {
      command.addAll(mongoClient.serverApi!.options);
    }
  }
}
