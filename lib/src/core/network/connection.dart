import 'dart:async';
import 'dart:io';

import 'package:mongo_db_driver/src/core/network/abstract/connection_base.dart';

import '../error/connection_exception.dart';

class Connection extends ConnectionBase {
  Connection(super.server) : super.protected();
  @override
  Future<void> internalConnect() async {
    Socket locSocket;
    try {
      locSocket = await Socket.connect(serverConfig.host, serverConfig.port);
    } catch (e) {
      // Socket connection - Connection refused from remote computer.
      var ex = ConnectionException(
          'Could not connect to ${serverConfig.hostUrl}\n- $e');
      throw ex;
    }

    setSocket(locSocket);
  }
}
