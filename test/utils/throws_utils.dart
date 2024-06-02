import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:test/test.dart' show throwsA;

var throwsMongoDartError = throwsA((e) => e is MongoDartError);
