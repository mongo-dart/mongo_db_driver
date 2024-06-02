import 'dart:async';
import 'dart:io';
import 'package:meta/meta.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/core/network/abstract/connection_events.dart';
import 'package:mongo_db_driver/src/core/network/connection.dart';

import 'package:mongo_db_driver/src/core/network/secure_connection.dart';
import 'package:mongo_db_driver/src/utils/generic_error.dart';
import 'package:logging/logging.dart';

import '../../../topology/server.dart';
import '../../../utils/events.dart';
import '../../info/server_config.dart';
import '../../message/handler/message_handler.dart';
import '../../message/mongo_message.dart';

const noSecureRequestError = 'The socket connection has been reset by peer.'
    '\nPossible causes:'
    '\n- Trying to connect to an ssl/tls encrypted database without specifiyng'
    '\n  either the query parm tls=true '
    'or the secure=true parameter in db.open()'
    '\n- The server requires a key certificate from the client, '
    'but no certificate has been sent'
    '\n- Others';

enum ConnectionState { closed, active, available }

int _uniqueIdentifier = 0;

Set<String> _legalEvents = <String>{
  extractType(Connected),
  extractType(ConnectionError),
  extractType(ConnectionActive),
  extractType(ConnectionClosed),
  extractType(ConnectionAvailable),
  extractType(ConnectionMessageReceived)
};

abstract class ConnectionBase with EventEmitter {
  @protected
  ConnectionBase.protected(this.server) : id = ++_uniqueIdentifier {
    legalEvents = _legalEvents;
  }

  factory ConnectionBase(Server server) {
    if (server.mongoClient.clientAuth?.isSecure ?? false) {
      return SecureConnection(server);
    }
    return Connection(server);
  }

  late int id;

  /// Is the reference to the server for which it has been opened
  Server server;

  /// If authentication is needed and it has been authenticated,
  ///  it is set to true
  bool isAuthenticated = false;
  ServerConfig get serverConfig => server.serverConfig;

  @protected
  final Logger log = Logger('Connection');

  Socket? socket;
  ConnectionState _state = ConnectionState.closed;
  Completer<MongoMessage>? _completer;

  bool get isClosed => _state == ConnectionState.closed;
  bool get isAvailable => _state == ConnectionState.available;
  bool get isActive => _state == ConnectionState.active;

  Future<void> connect() async {
    if (!isClosed) {
      await _closeOnError(MongoDartError(
          'Call to connect(), but the connection is alreay open'));
    }

    return internalConnect();
  }

  Future<void> _closeOnError(GenericError error) async {
    await _closeConnection();
    await emit(ConnectionError(id, error));
    log.severe(error.originalErrorMessage);
    _completer == null ? throw error : _completer!.completeError(error);
  }

  Future<void> _closeConnection() async {
    if (!isClosed) {
      await emit(ConnectionClosed(id));
      isAuthenticated = false;
      _state = ConnectionState.closed;
    }
    if (socket != null) {
      await socket!.flush();
      await socket!.close();
      socket = null;
    }
  }

  @protected
  Future<void> internalConnect();

  void setSocket(Socket newSocket) {
    socket = newSocket;

    socket!
        .transform<MongoMessage>(MessageHandler().transformer)
        .listen(receiveReply, onError: (error, st) async {
      await _closeOnError(
          MongoDartError('Socket error $error', stackTrace: st));
    },
            //cancelOnError: true,
            // onDone is not called in any case after onData or OnError,
            // it is called when the socket closes, i.e. it is an error.
            // Possible causes:
            // * Trying to connect to a tls encrypted Database
            //   without specifing tls=true in the query parms or setting
            //   the secure parameter to true in db.open()
            onDone: () async {
      await _closeOnError(MongoDartError(noSecureRequestError));
    });
    // ignore: unawaited_futures
    socket!.done.catchError((error) async {
      await _closeOnError(MongoDartError('Socket error $error'));
    });
    emit(Connected(id));
    _state = ConnectionState.available;
    emit(ConnectionAvailable(id));
  }

  Future<MongoMessage> execute(MongoMessage message) async {
    if (_state == ConnectionState.closed) {
      await _closeOnError(
          MongoDartError('Invalid state: Connection already closed.'));
    } else if (_state == ConnectionState.active) {
      await _closeOnError(
          MongoDartError('Invalid state: Connection already processing.'));
    }
    await emit(ConnectionActive(id));

    var finalMessage = <int>[];
    finalMessage.addAll(message.serialize().byteList);

    log.finest(() => 'Submitting message $message');
    _completer = Completer<MongoMessage>();

    socket!.add(finalMessage);

    return _completer!.future;
  }

  Future<void> receiveReply(MongoMessage reply) async {
    log.finest(() => reply.toString());

    if (_completer != null) {
      log.fine(() => 'Completing $reply');
      await emit(ConnectionMessageReceived(id, reply));
      await emit(ConnectionAvailable(id));
      _completer!.complete(reply);
    } else {
      await _closeOnError(
          MongoDartError('Unexpected respondTo: ${reply.responseTo} $reply'));
    }
  }

  Future<void> close() async => _closeConnection();
}
