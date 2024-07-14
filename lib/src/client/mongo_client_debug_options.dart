import 'package:logging/logging.dart';

/// Describes all possible Debug options for the mongo client
class MongoClientDebugOptions {
  /// Specifies the log level for the command execution module
  Level? commandExecutionLogLevel = Level.OFF;
}
