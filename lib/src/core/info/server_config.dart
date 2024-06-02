//part of mongo_dart;

import 'dart:typed_data';

class ServerConfig {
  String host;
  int port;
  @Deprecated('Use Client.clientAuth instead')
  bool isSecure = false;
  @Deprecated('Use Client.clientAuth instead')
  bool tlsAllowInvalidCertificates = false;
  @Deprecated('Use Client.clientAuth instead')
  Uint8List? tlsCAFileContent;
  @Deprecated('Use Client.clientAuth instead')
  Uint8List? tlsCertificateKeyFileContent;
  @Deprecated('Use Client.clientAuth instead')
  String? tlsCertificateKeyFilePassword;
  @Deprecated('Use Client.clientAuth instead')
  String? userName;
  @Deprecated('Use Client.clientAuth instead')
  String? password;

  @Deprecated('Use Client.clientAuth instead')
  bool isAuthenticated = false;

  ServerConfig({this.host = '127.0.0.1', this.port = 27017});
  String get hostUrl => '$host:${port.toString()}';
}
