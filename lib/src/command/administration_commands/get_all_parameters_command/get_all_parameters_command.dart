import 'package:mongo_db_driver/src/command/base/db_admin_command_operation.dart';
import '../../../client/client_exp.dart';
import 'get_all_parameters_options.dart';

base class GetAllParametersCommand extends DbAdminCommandOperation {
  GetAllParametersCommand(MongoClient client,
      {super.session,
      GetAllParametersOptions? getAllParametersOptions,
      Map<String, Object>? rawOptions})
      : super(client, <String, dynamic>{
          keyGetParameter: '*'
        }, options: <String, dynamic>{
          ...?getAllParametersOptions?.options,
          ...?rawOptions
        });
}
