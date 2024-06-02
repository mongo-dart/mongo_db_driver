import 'package:mongo_db_driver/src/command/base/command_operation.dart';
import 'package:mongo_db_driver/src/command/base/operation_base.dart';
import 'package:mongo_db_driver/src/utils/map_keys.dart';

import '../../../database/base/mongo_database.dart';
import '../../../database/base/mongo_collection.dart';
import 'create_index_options.dart';

const Set keysToOmit = <String>{
  'name',
  'key',
  'writeConcern',
  'w',
  'wtimeout',
  'j',
  'fsync',
  'readPreference',
  'session'
};

base class CreateIndexOperation extends CommandOperation {
  Object fieldOrSpec;
  late Map<String, Object> indexes;

  CreateIndexOperation(MongoDatabase db, MongoCollection collection,
      this.fieldOrSpec, CreateIndexOptions? indexOptions,
      {super.session, Map<String, Object>? rawOptions})
      : super(db, {},
            <String, dynamic>{...?indexOptions?.options, ...?rawOptions},
            collection: collection, aspect: Aspect.writeOperation) {
    // parseIndexOptions alway returns a filled "keyName" and a "keyFieldHash"
    // elements
    var indexParameters = parseIndexOptions(fieldOrSpec);
    final indexName = options[keyName] != null && options[keyName] is String
        ? options[keyName] as String
        : indexParameters[keyName] as String;
    indexes = {keyName: indexName, keyKey: indexParameters[keyFieldHash]!};
    options.remove(keyName);
  }

  @override
  Command $buildCommand() {
    var indexes = this.indexes;

    // merge all options
    var added = <String>[];
    for (var optionName in options.keys) {
      if (!keysToOmit.contains(optionName) && options[optionName] != null) {
        indexes[optionName] = options[optionName]!;
        added.add(optionName);
      }
    }
    for (var optionName in added) {
      options.remove(optionName);
    }

    // Create command, apply write concern to command
    return <String, dynamic>{
      keyCreateIndexes: collection!.collectionName,
      keyCreateIndexesArgument: [indexes]
    };
  }
}

Map<String, Object> parseIndexOptions(Object fieldOrSpec) {
  var fieldHash = <String, dynamic>{};
  var indexes = <String>[];
  Iterable? keys;

// Get all the fields accordingly
  if (fieldOrSpec is String) {
// 'type'
    indexes.add(_fieldIndexName(fieldOrSpec, '1'));
    fieldHash[fieldOrSpec] = 1;
  } else if (fieldOrSpec is List) {
    for (Object object in fieldOrSpec) {
      if (object is String) {
// [{location:'2d'}, 'type']
        indexes.add(_fieldIndexName(object, '1'));
        fieldHash[object] = 1;
      } else if (object is List) {
// [['location', '2d'],['type', 1]]
        indexes.add(
            _fieldIndexName(object[0] as String, (object[1] ?? '1') as String));
        fieldHash[object[0]] = object[1] ?? '1';
      } else if (object is Map) {
// [{location:'2d'}, {type:1}]
        keys = object.keys;
        for (String key in keys) {
          indexes.add(_fieldIndexName(key, object[key] as String));
          fieldHash[key] = object[key];
        }
      } else {
// undefined (ignore)
      }
    }
  } else if (fieldOrSpec is Map) {
// {location:'2d', type:1}
    keys = fieldOrSpec.keys;
    for (String key in keys) {
      var indexDirection = '${fieldOrSpec[key]}';
      indexes.add(_fieldIndexName(key, indexDirection));
      fieldHash[key] = fieldOrSpec[key];
    }
  }

  return {
    keyName: indexes.join('_'),
    if (keys != null) keyKeys: keys,
    keyFieldHash: fieldHash
  };
}

String _fieldIndexName(String fieldName, String sort) => '${fieldName}_$sort';
