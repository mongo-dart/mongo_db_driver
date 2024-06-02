import '../../../utils/generic_error.dart';
import '../../../utils/events.dart';
import '../../message/mongo_message.dart';

Set<String> connectionEvents = {
  extractType(Connected),
  extractType(ConnectionError),
  extractType(ConnectionClosed),
  extractType(ConnectionActive),
  extractType(ConnectionAvailable)
};

abstract class ConnectionEvent extends Event {
  ConnectionEvent(this.id);
  int id;
}

/// This Message is sent when the Connection is connected
class Connected extends ConnectionEvent {
  Connected(super.id);
}

/// This Message is sent when an Error is detected
class ConnectionError extends ConnectionEvent {
  ConnectionError(super.id, this.error);

  GenericError error;
}

/// This Message is sent when the connection is closed
class ConnectionClosed extends ConnectionEvent {
  ConnectionClosed(super.id);
}

/// This Message is sent when the connection is in use
class ConnectionActive extends ConnectionEvent {
  ConnectionActive(super.id);
}

/// This Message is sent when the connection is available again after
/// beeign used
class ConnectionAvailable extends ConnectionEvent {
  ConnectionAvailable(super.id);
  MongoMessage? reply;
}

/// This Message is sent when the connection receive a response
class ConnectionMessageReceived extends ConnectionEvent {
  ConnectionMessageReceived(super.id, this.reply);
  MongoMessage? reply;
}
