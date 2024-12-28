# Changelog Mongo Db Driver

## 0.1.0-1.4.dev

- Update Dependencies
- Removed Library names (Lint)
- Changes to Grid Fs to fix problem with files bigger than 2GB.
  1) Changed length in GridFSFile from int? to Int64
  2) Changed chunkSize in GridFSFile from int to Int32
  3) Changed GridFS.defaultChunkSize from int to Int32
  4) Removed Validate() method from GridFSFile (MD5 no more managed)

## 0.1.0-1.3.dev

- Fixed bson and mongo_db_query exports

## 0.1.0-1.2.dev

- Moved Document types in package mongo_db_query
- Export Reorganization
- Fixed issue with mongodb+srv connection String
- Using Union Base Classes and Error Base classes from type_utils packahge
- Direct Connection
- Server Monitoring (no log yet)
- Server Description

## 0.1.0-1.1.dev

- Aggregate - Collection methods and commands

## 1.0.0-1.0.dev

New version
