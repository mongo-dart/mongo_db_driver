library mongo_db_query_test;

import 'package:test/test.dart';
import 'package:bson/bson.dart';
import 'package:mongo_db_query/mongo_db_query.dart';

void testQueryExpressionCreation() {
  var selector = where;
  expect(selector.rawFilter, isEmpty);
}

void testQueryExpressionOnObjectId() {
  var id = ObjectId();
  var selector = where..id(id);
  expect(selector.rawFilter.length, greaterThan(0));
  expect(
      selector.rawFilter,
      equals({
        '_id': {r'$eq': id}
      }));
}

void testQueries() {
  var selector = where
    ..$gt('my_field', 995)
    ..sortBy('my_field');
  expect(selector.rawFilter, {
    'my_field': {r'$gt': 995}
  });
  expect(selector.sortExp.build(), {'my_field': 1});
  selector = where
    ..inRange('my_field', 700, 703, minInclude: false)
    ..sortBy('my_field');
  expect(selector.rawFilter, {
    'my_field': {r'$gt': 700, r'$lt': 703},
  });
  expect(selector.sortExp.build(), {'my_field': 1});
  selector = where
    ..$eq('my_field', 17)
    ..fields.includeField('str_field');
  expect(selector.rawFilter, {
    'my_field': {r'$eq': 17}
  });
  expect(selector.fields.build(), {'str_field': 1});
  selector = where
    ..sortBy('a')
    ..skip(300);
  expect(selector.sortExp.build(), {'a': 1});
  expect(selector.getSkip(), 300);
  /* selector = where.hint('bar').hint('baz', descending: true).explain();
  expect(
      selector.map,
      equals({
        '\$query': {},
        '\$hint': {'bar': 1, 'baz': -1},
        '\$explain': true
      })); */
  /*  selector = where.hintIndex('foo');
  expect(selector.map, equals({'\$query': {}, '\$hint': 'foo'})); */
}

void testQueryComposition() {
  var selector = where
    ..$gt('a', 995)
    ..$eq('b', 'bbb');
  expect(
      selector.rawFilter,
      equals({
        'a': {r'$gt': 995},
        'b': {r'$eq': 'bbb'}
      }));
  selector = where
    ..$gt('a', 995)
    ..$lt('a', 1000);
  expect(
      selector.rawFilter,
      equals({
        'a': {r'$gt': 995, r'$lt': 1000}
      }));
  selector = where
    ..$gt('a', 995)
    ..$and
    ..open
    ..$lt('b', 1000)
    ..$or
    ..$gt('c', 2000)
    ..close;
  expect(selector.rawFilter, {
    'a': {'\$gt': 995},
    '\$or': [
      {
        'b': {'\$lt': 1000}
      },
      {
        'c': {'\$gt': 2000}
      }
    ]
  });
  selector = where
    ..open
    ..$lt('b', 1000)
    ..$or
    ..$gt('c', 2000)
    ..close
    ..$and
    ..$gt('a', 995);
  expect(selector.rawFilter, {
    '\$or': [
      {
        'b': {'\$lt': 1000}
      },
      {
        'c': {'\$gt': 2000}
      }
    ],
    'a': {'\$gt': 995}
  });
  selector = where
    ..open
    ..$lt('b', 1000)
    ..$or
    ..$gt('c', 2000)
    ..close
    ..$gt('a', 995);
  expect(selector.rawFilter, {
    '\$or': [
      {
        'b': {'\$lt': 1000}
      },
      {
        'c': {'\$gt': 2000}
      }
    ],
    'a': {'\$gt': 995}
  });
  selector = where
    ..$lt('b', 1000)
    ..$or
    ..$gt('c', 2000)
    ..$or
    ..$gt('a', 995);
  expect(selector.rawFilter, {
    '\$or': [
      {
        'b': {'\$lt': 1000}
      },
      {
        'c': {'\$gt': 2000}
      },
      {
        'a': {'\$gt': 995}
      }
    ]
  });
  selector = where
    ..$eq('price', 1.99)
    ..$and
    ..open
    ..$lt('qty', 20)
    ..$or
    ..$eq('sale', true)
    ..close;
  expect(selector.rawFilter, {
    'price': {r'$eq': 1.99},
    '\$or': [
      {
        'qty': {'\$lt': 20}
      },
      {
        'sale': {r'$eq': true}
      }
    ]
  });
  selector = where
    ..$eq('price', 1.99)
    ..$and
    ..$lt('qty', 20)
    ..$and
    ..$eq('sale', true);
  expect(selector.rawFilter, {
    'price': {r'$eq': 1.99},
    'qty': {'\$lt': 20},
    'sale': {r'$eq': true}
  });
  selector = where
    ..$eq('price', 1.99)
    ..$lt('qty', 20)
    ..$eq('sale', true);
  expect(selector.rawFilter, {
    'price': {r'$eq': 1.99},
    'qty': {'\$lt': 20},
    'sale': {r'$eq': true}
  });
}

void testUpdateExpression() {
  var modifier = modify
    ..$set('a', 995)
    ..$set('b', 'bbb');
  expect(
      modifier.build(),
      equals({
        r'$set': {'a': 995, 'b': 'bbb'}
      }));
  modifier = modify
    ..$unset('a')
    ..$unset('b');
  expect(
      modifier.build(),
      equals({
        r'$unset': {'a': 1, 'b': 1}
      }));
}

void testGetQueryString() {
  var selector = where..$eq('foo', 'bar');
  expect(selector.getQueryString(), r'{"foo":{"$eq":"bar"}}');
  selector = where..$lt('foo', 2);
  expect(selector.getQueryString(), r'{"foo":{"$lt":2}}');
  var id = ObjectId();
  selector = where..id(id);
  expect(selector.getQueryString(), '{"_id":{"\$eq":"${id.oid}"}}');
}

void main() {
  test('testQueryExpressionCreation', testQueryExpressionCreation);
  test('testQueryExpressionOnObjectId', testQueryExpressionOnObjectId);
  test('testQueries', testQueries);
  test('testQueryComposition', testQueryComposition);
  test('testModifierBuilder', testUpdateExpression);
  test('testGetQueryString', testGetQueryString);
}
