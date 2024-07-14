import '../../../client/mongo_client.dart';
import '../../../utils/map_keys.dart';
import '../../base/db_admin_command_operation.dart';
import 'list_databases_options.dart';

base class ListDatabasesCommand extends DbAdminCommandOperation {
  ListDatabasesCommand(MongoClient client,
      {super.session,
      ListDatabasesOptions? listDatabasesOptions,
      Map<String, Object>? rawOptions})
      : super(client, <String, dynamic>{
          keyListDatabases: '1'
        }, options: <String, dynamic>{
          ...?listDatabasesOptions?.options,
          ...?rawOptions
        });
}
