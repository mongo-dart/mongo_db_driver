import 'package:mongo_db_query/mongo_db_query.dart';

import '../database/database.dart';
import 'grid_in.dart';
import 'grid_out.dart';

class GridFS {
  static int defaultChunkSize = 256 * 1024;
  static int maxChunkSize = (3.5 * 1000 * 1000).toInt();

  MongoDatabase database;
  MongoCollection files;
  MongoCollection chunks;
  String bucketName;

  GridFS(this.database, [String collection = 'fs'])
      : files = database.collection('$collection.files'),
        chunks = database.collection('$collection.chunks'),
        bucketName = collection;

  // T O D O (tsander): Ensure index.

  Stream<Map<String, dynamic>> getFileList(QueryExpression queryExpression) =>
      files.find(filter: queryExpression..sortBy({'filename': 1}));

  Future<GridOut?> findOne(selector) async {
    //var completer = Completer<GridOut>();
    var file = await files.findOne(filter: selector); //.then((file) {

    if (file == null) {
      return null;
    }
    return GridOut(this, file); //..setGridFS(this);
    //GridOut? result;
    //if (file != null) {
    //  result = GridOut(file);
    //  result.setGridFS(this);
    //}
    //  completer.complete(result);
    //});
    //return completer.future;
  }

  Future<GridOut?> getFile(String fileName) async =>
      findOne(where..$eq('filename', fileName));

  GridIn createFile(Stream<List<int>> input, String filename,
          [Map<String, dynamic>? extraData]) =>
      GridIn(this, filename, input, extraData);

  /// **Beware!** This method removes all the documents in this bucket
  Future<void> clearBucket() async {
    await files.deleteMany();
    await chunks.deleteMany();
  }

  /// **Beware!** This method drops this bucket
  Future<void> dropBucket() async {
    await files.drop();
    await chunks.drop();
  }
}
