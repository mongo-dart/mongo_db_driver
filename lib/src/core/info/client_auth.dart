//part of mongo_dart;

import 'dart:typed_data';

class ClientAuth {
  bool isSecure;
  bool tlsAllowInvalidCertificates;
  Uint8List? tlsCAFileContent;
  Uint8List? tlsCertificateKeyFileContent;
  String? tlsCertificateKeyFilePassword;

  String? userName;
  String? password;

  bool isAuthenticated = false;

  ClientAuth(
      {bool? isSecure,
      bool? tlsAllowInvalidCertificates,
      this.tlsCAFileContent,
      this.tlsCertificateKeyFileContent,
      this.tlsCertificateKeyFilePassword})
      : isSecure = isSecure ?? false,
        tlsAllowInvalidCertificates = tlsAllowInvalidCertificates ?? false;
}
