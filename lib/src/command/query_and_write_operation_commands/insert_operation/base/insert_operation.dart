import 'package:bson/bson.dart';
import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../core/error/mongo_dart_error.dart';
import '../../../../database/database_exp.dart';
import '../../../../utils/map_keys.dart';
import '/src/command/base/command_operation.dart';
import '/src/command/base/operation_base.dart';

import '../../../../session/client_session.dart';
import '../open/insert_operation_open.dart';
import '../v1/insert_operation_v1.dart';
import 'insert_options.dart';

typedef InsertRec = (
  MongoDocument serverDocument,
  List<MongoDocument> insertedDocuments,
  List ids
);

abstract base class InsertOperation extends CommandOperation {
  @protected
  InsertOperation.protected(MongoCollection collection, this.documents,
      {super.session, InsertOptions? insertOptions, Options? rawOptions})
      : ids = List.filled(documents.length, null),
        super(
            collection.db,
            {},
            <String, dynamic>{
              ...?insertOptions?.getOptions(collection.db),
              ...?rawOptions
            },
            collection: collection,
            aspect: Aspect.writeOperation) {
    if (documents.isEmpty) {
      throw ArgumentError('Documents required in insert operation');
    }

    for (var idx = 0; idx < documents.length; idx++) {
      documents[idx][key_id] ??= ObjectId();
      ids[idx] = documents[idx][key_id];
    }
  }

  factory InsertOperation(
      MongoCollection collection, List<MongoDocument> documents,
      {ClientSession? session,
      InsertOptions? insertOptions,
      Options? rawOptions}) {
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return InsertOperationV1(collection, documents,
              session: session,
              insertOptions: insertOptions?.toV1,
              rawOptions: rawOptions);
        // ignore: unreachable_switch_default
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return InsertOperationOpen(collection, documents,
        session: session,
        insertOptions: insertOptions?.toOpen,
        rawOptions: rawOptions);
  }

  List<MongoDocument> documents;
  List ids;

  @override
  Command $buildCommand() => <String, dynamic>{
        keyInsert: collection!.collectionName,
        keyDocuments: documents
      };

  Future<InsertRec> executeInsert() async {
    var ret = await super.process();
    return (ret, documents, ids);
  }
}
