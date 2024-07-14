import 'package:mongo_db_driver/mongo_db_driver.dart' show MongoDartError;
import 'package:test/test.dart' show Matcher, TypeMatcher, throwsA;

final Matcher throwsMongoDartError = throwsA(TypeMatcher<MongoDartError>());
