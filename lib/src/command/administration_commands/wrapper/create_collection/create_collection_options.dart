import '../../create_command/create_options.dart';

class CreateCollectionOptions extends CreateOptions {
  CreateCollectionOptions({
    super.capped,
    super.size,
    super.autoIndexId,
    super.max,
    super.storageEngine,
    super.validator,
    super.validationLevel,
    super.validationAction,
    Map<String, Object>? super.indexOptionDefaults,
    super.collation,
    super.writeConcern,
    super.comment,
  });
}
