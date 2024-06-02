import 'package:mongo_db_driver/src/core/error/mongo_dart_error.dart';
import 'package:test/test.dart' show Matcher, TypeMatcher, throwsA;

final Matcher throwsMongoDartError = throwsA(TypeMatcher<MongoDartError>());
