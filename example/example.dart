import 'dart:io' show Platform;

import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

String host = Platform.environment['MONGO_DART_DRIVER_HOST'] ?? '127.0.0.1';
String port = Platform.environment['MONGO_DART_DRIVER_PORT'] ?? '27017';

void main() async {
  var client = MongoClient('mongodb://$host:$port/mongo_dart-blog');
  await client.connect();
  final db = client.db();
  // Example url for Atlas connection
  /* var db = Db('mongodb://<atlas-user>:<atlas-password>@'
      'cluster0-shard-00-02.xtest.mongodb.net:27017,'
      'cluster0-shard-00-01.xtest.mongodb.net:27017,'
      'cluster0-shard-00-00.xtest.mongodb.net:27017/'
      'mongo_dart-blog?authSource=admin&compressors=disabled'
      '&gssapiServiceName=mongodb&replicaSet=atlas-stcn2i-shard-0'
      '&ssl=true'); */
  var authors = <String, Map>{};
  var users = <String, Map>{};
  await db.dropDatabase();
  print('====================================================================');
  print('>> Adding Authors');
  var collection = db.collection('authors');
  await collection.insertMany([
    {
      'name': 'William Shakespeare',
      'email': 'william@shakespeare.com',
      'age': 587
    },
    {'name': 'Jorge Luis Borges', 'email': 'jorge@borges.com', 'age': 123}
  ]);
  await db.ensureIndex('authors',
      name: 'meta', keys: {'_id': 1, 'name': 1, 'age': 1});
  await collection.find().forEach((v) {
    print(v);
    authors[v['name'].toString()] = v;
  });
  print('====================================================================');
  print('>> Authors ordered by age ascending');
  await collection.find(filter: where..sortBy('age')).forEach(
      (auth) => print("[${auth['name']}]:[${auth['email']}]:[${auth['age']}]"));
  print('====================================================================');
  print('>> Adding Users');
  var usersCollection = db.collection('users');
  await usersCollection.insertMany([
    {'login': 'jdoe', 'name': 'John Doe', 'email': 'john@doe.com'},
    {'login': 'lsmith', 'name': 'Lucy Smith', 'email': 'lucy@smith.com'}
  ]);
  await db.ensureIndex('users', keys: {'login': -1});
  await usersCollection.find().forEach((user) {
    users[user['login'].toString()] = user;
    print(user);
  });
  print('====================================================================');
  print('>> Users ordered by login descending');
  await usersCollection.find(filter: where..sortBy({'login': -1})).forEach(
      (user) =>
          print("[${user['login']}]:[${user['name']}]:[${user['email']}]"));
  print('====================================================================');
  print('>> Adding articles');
  var articlesCollection = db.collection('articles');
  await articlesCollection.insertMany([
    {
      'title': 'Caminando por Buenos Aires',
      'body': 'Las callecitas de Buenos Aires tienen ese no se que...',
      'author_id': authors['Jorge Luis Borges']?['_id']
    },
    {
      'title': 'I must have seen thy face before',
      'body': 'Thine eyes call me in a new way',
      'author_id': authors['William Shakespeare']?['_id'],
      'comments': [
        {'user_id': users['jdoe']?['_id'], 'body': 'great article!'}
      ]
    }
  ]);
  print('====================================================================');
  print('>> Articles ordered by title ascending');
  await articlesCollection
      .find(filter: where..sortBy('title'))
      .forEach((article) {
    print("[${article['title']}]:[${article['body']}]:"
        "[${article['author_id'].toHexString()}]");
  });
  await client.close();
}
