import 'dart:async';
import 'dart:io';

import 'package:mongo_db_driver/src/core/network/abstract/connection_base.dart';

import '../error/connection_exception.dart';

class SecureConnection extends ConnectionBase {
  static bool _caCertificateAlreadyInHash = false;

  SecureConnection(super.server) : super.protected();

  @override
  Future<void> internalConnect() async {
    Socket locSocket;
    try {
      var securityContext = SecurityContext.defaultContext;
      if (server.mongoClient.clientAuth?.tlsCAFileContent != null &&
          !_caCertificateAlreadyInHash) {
        securityContext.setTrustedCertificatesBytes(
            server.mongoClient.clientAuth!.tlsCAFileContent!);
      }
      if (server.mongoClient.clientAuth?.tlsCertificateKeyFileContent != null) {
        securityContext
          ..useCertificateChainBytes(
              server.mongoClient.clientAuth!.tlsCertificateKeyFileContent!)
          ..usePrivateKeyBytes(
              server.mongoClient.clientAuth!.tlsCertificateKeyFileContent!,
              password:
                  server.mongoClient.clientAuth?.tlsCertificateKeyFilePassword);
      }

      locSocket = await SecureSocket.connect(
          serverConfig.host, serverConfig.port, context: securityContext,
          onBadCertificate: (certificate) {
        // couldn't find here if the cause is an hostname mismatch
        return server.mongoClient.clientAuth!.tlsAllowInvalidCertificates;
      });
    } on TlsException catch (e) {
      if (e.osError?.message
              .contains('CERT_ALREADY_IN_HASH_TABLE(x509_lu.c:356)') ??
          false) {
        _caCertificateAlreadyInHash = true;
        return connect();
      }
      var ex = ConnectionException(
          'Could not connect to ${serverConfig.hostUrl}\n- $e');
      throw ex;
    } catch (e) {
      var ex = ConnectionException(
          'Could not connect to ${serverConfig.hostUrl}\n- $e');
      throw ex;
    }

    setSocket(locSocket);
  }
}
