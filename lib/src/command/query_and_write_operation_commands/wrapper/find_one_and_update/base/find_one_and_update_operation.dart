import 'package:meta/meta.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

import '../../../../../core/error/mongo_dart_error.dart';
import '../../../../../database/database_exp.dart';
import '../../../../../server_api_version.dart';
import '../../../../../session/session_exp.dart';
import '../../../../../unions/hint_union.dart';
import '../../../../../unions/projection_union.dart';
import '../../../../../unions/query_union.dart';
import '../../../../../unions/sort_union.dart';
import '../../../../base/operation_base.dart';
import '../../../../command_exp.dart';
import '../../../update_operation/base/update_union.dart';
import '../open/find_one_and_update_operation_open.dart';
import '../v1/find_one_and_update_operation_v1.dart';
import 'find_one_and_update_options.dart';

typedef FindOneAndUpdateDocumentRec = (
  FindAndModifyResult findAndModifyResult,
  MongoDocument serverDocument
);

abstract base class FindOneAndUpdateOperation extends FindAndModifyOperation {
  @protected
  FindOneAndUpdateOperation.protected(super.collection,
      {super.query,
      super.update,
      super.fields,
      super.sort,
      super.upsert,
      super.returnNew,
      super.arrayFilters,
      super.session,
      super.hint,
      FindOneAndUpdateOptions? findOneAndUpdateOptions,
      super.rawOptions})
      : super.protected(
            remove: false, findAndModifyOptions: findOneAndUpdateOptions);

  factory FindOneAndUpdateOperation(MongoCollection collection,
      {QueryUnion? query,
      UpdateUnion? update,
      ProjectionUnion? fields,
      SortUnion? sort,
      bool? upsert,
      bool? returnNew,
      List<ArrayFilter>? arrayFilters,
      ClientSession? session,
      HintUnion? hint,
      FindOneAndUpdateOptions? findOneAndUpdateOptions,
      Options? rawOptions}) {
    query ??= QueryUnion(emptyQueryFilter);
    if (collection.serverApi != null) {
      switch (collection.serverApi!.version) {
        case ServerApiVersion.v1:
          return FindOneAndUpdateOperationV1(collection,
              query: query,
              update: update,
              fields: fields,
              sort: sort,
              upsert: upsert,
              returnNew: returnNew,
              arrayFilters: arrayFilters,
              session: session,
              findOneAndUpdateOptions:
                  findOneAndUpdateOptions?.toFindOneAndUpdateOptionsV1,
              hint: hint,
              rawOptions: rawOptions);
        default:
          throw MongoDartError(
              'Stable Api ${collection.serverApi!.version} not managed');
      }
    }
    return FindOneAndUpdateOperationOpen(collection,
        query: query,
        update: update,
        fields: fields,
        sort: sort,
        upsert: upsert,
        returnNew: returnNew,
        arrayFilters: arrayFilters,
        session: session,
        findOneAndUpdateOptions:
            findOneAndUpdateOptions?.toFindOneAndUpdateOptionsOpen,
        hint: hint,
        rawOptions: rawOptions);
  }

  Future<MongoDocument> executeFindOneAndUpdate() async => process();

  Future<FindOneAndUpdateDocumentRec> executeDocument() async {
    var ret = await executeFindOneAndUpdate();
    return (FindAndModifyResult(ret), ret);
  }
}
