import 'package:type_utils/error.dart';

class MongoDbError extends ApplicationError {
  MongoDbError(super.errorMessage, {super.errorCode, super.stackTrace});
}

class MongoDbTemplateError extends TemplateError {
  MongoDbTemplateError(super.originalErrorMessage,
      {required super.templateMessageCode,
      super.errorCode,
      super.stackTrace,
      super.values});
}
