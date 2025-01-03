import 'dart:typed_data';

import 'package:bson/bson.dart';
import 'package:fixnum/fixnum.dart';

import '../core/error/mongo_dart_error.dart';
import 'chunk_handler.dart';
import 'grid_fs_file.dart';
import 'gridfs.dart';

class GridIn extends GridFSFile {
  late Stream<Uint8List> input;
  bool savedChunks = false;
  int currentChunkNumber = 0;
  Int64 totalBytes = Int64();

  //@override
  //String? filename;

  /// Used for MD5 calculation, now deprecated
  //Uint8List contentToDigest = Uint8List(0);
  GridIn(GridFS fs, String filename, Stream<List<int>> inputStream,
      [Map<String, dynamic>? extraData])
      : super(fs, extraData) {
    id = ObjectId();
    input = ChunkHandler(chunkSize).transformer.bind(inputStream);
    uploadDate = DateTime.now();
    // ignore: prefer_initializing_formals
    this.filename = filename;
  }

  @override
  Future<Map<String, dynamic>> save([Int32? chunkSize]) async {
    if (!savedChunks) {
      return saveChunks(chunkSize);
    }
    return {'ok': 1.0};
  }

  Future<Map<String, dynamic>> saveChunks([Int32? chunkSize]) async {
    chunkSize ??= this.chunkSize;

    if (savedChunks) {
      throw MongoDartError('chunks already saved!');
    }
    if (chunkSize <= 0 || chunkSize > GridFS.maxChunkSize) {
      throw MongoDartError(
          'chunkSize must be greater than zero and less than or equal to GridFS.MAX_CHUNKSIZE');
    }

    await for (var data in input) {
      await dumpBuffer(data);
    }

    return finishData();
  }

  Future<Map<String, dynamic>> dumpBuffer(Uint8List writeBuffer) async {
    if (writeBuffer.isEmpty) {
      // Chunk is empty, may be last chunk
      return <String, dynamic>{};
    }

    var chunk = <String, dynamic>{
      'files_id': id,
      'n': currentChunkNumber,
      'data': BsonBinary.from(writeBuffer)
    };
    currentChunkNumber++;

    totalBytes += writeBuffer.length;
    /*  var actualLength = contentToDigest.length;
    var contentTemp = Uint8List(totalBytes);
    contentTemp.setAll(0, contentToDigest);
    contentTemp.setAll(actualLength, writeBuffer);
    contentToDigest = contentTemp; */
    // TODO update this
    var (_, retValue, _, _) = await fs.chunks.insertOne(chunk);
    return retValue;
  }

  Future<Map<String, dynamic>> finishData() async {
    if (!savedChunks) {
      //md5 = crypto.md5.convert(contentToDigest).toString();
      length = totalBytes;
      savedChunks = true;
    }
    return super.save();
  }
}
