import 'package:mongo_db_driver/src/mongo_client.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

void main() async {
  var client = MongoClient('mongodb://127.0.0.1/mongo_dart-test');
  await client.connect();
  var db = client.db();
  var collection = db.collection('test-utf8');
  await collection.deleteMany();
  await collection.insertOne({
    'Имя': 'Вадим',
    'Фамилия': 'Цушко',
    'Профессия': 'Брадобрей',
    'Шаблон': RegExp('^.adim\$')
  });
  var v = await collection.findOne();
  print(
      'Utf8 encoding demonstration. I18 strings may be used not only as values but also as keys');
  print(v);
  v = await collection.findOne(filter: where..$eq('Имя', 'Вадим'));
  print('Filtered by query()..\$eq(): $v');
  v = await collection.findOne(
      filter: where..$regex('Имя', '^..ДИМ\$', caseInsensitive: true));
  print('Filtered by query().match(): $v');
  await client.close();
}
