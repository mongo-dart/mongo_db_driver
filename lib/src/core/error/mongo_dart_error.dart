import '../../utils/generic_error.dart';

class MongoDartError extends GenericError {
  final int? mongoCode;
  final String? errorCodeName;

  MongoDartError(super.message,
      {this.mongoCode, String? errorCode, this.errorCodeName, super.stackTrace})
      : super(
            errorCode: errorCode ?? (mongoCode != null ? '$mongoCode' : null));

  @override
  String toString() => 'MongoDart Error: $originalErrorMessage';

  String get message => originalErrorMessage;
}
