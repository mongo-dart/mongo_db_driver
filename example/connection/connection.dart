import 'package:logging/logging.dart'
    show Level, LogRecord, Logger, hierarchicalLoggingEnabled;
import 'package:mongo_db_driver/src/client/mongo_client.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';

const dbName = 'mongo-dart-example';
const dbAddress = 'localhost';

const defaultUri = 'mongodb://$dbAddress:27017/$dbName';

void main() async {
  hierarchicalLoggingEnabled = true;
  Logger('Mongoconnection example').level = Level.INFO;

  void listener(LogRecord r) {
    var name = r.loggerName;
    print('${r.time}: $name: ${r.message}');
  }

  Logger.root.onRecord.listen(listener);

  var client = MongoClient(defaultUri);
  await client.connect();

  Future cleanupDatabase() async {
    await client.close();
  }

  await cleanupDatabase();
}
