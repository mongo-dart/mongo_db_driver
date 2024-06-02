import 'package:mongo_db_driver/mongo_db_driver.dart';
import 'package:mongo_db_driver/src/command/base/db_admin_command_operation.dart';
import 'get_parameter_options.dart';

base class GetParameterCommand extends DbAdminCommandOperation {
  GetParameterCommand(MongoClient client, String parameterName,
      {super.session,
      GetParameterOptions? getParameterOptions,
      Map<String, Object>? rawOptions})
      : super(client, <String, dynamic>{
          keyGetParameter: 1,
          parameterName: 1
        }, options: <String, dynamic>{
          ...?getParameterOptions?.options,
          ...?rawOptions
        });
}
