import 'dart:async';

import 'package:test/test.dart';
import 'package:mongo_db_driver/mongo_db_driver.dart';

class Emitter with EventEmitter {}

class TestEvent1 extends Event {
  String message = '';
}

class TestEvent2 extends Event {
  int count = 0;
}

void main() {
  Emitter emitter;

  group('Utils', () {
    test('extractType', () {
      expect(extractType<TestEvent1>(), 'TestEvent1');
      expect(extractType(TestEvent1), 'TestEvent1');
      expect(extractType(TestEvent1()), 'TestEvent1');
    });
  });
  group('Event Emitter', () {
    test('Test subType', () async {
      var events = <Event>[];
      var event1 = TestEvent1();
      events.add(event1);
      var eventFunctions = <Function(Event)>[];
      eventFunctions.add((Event e) => 1);
      //eventFunctions.add((TestEvent1 e) => 1);
    });
    test('Add Legal Event', () async {
      emitter = Emitter();
      emitter.addLegalEvent<TestEvent1>();

      expect(emitter.legalEvents.length, 2);
      expect(emitter.legalEvents.first, 'ErrorMonitor');
      expect(emitter.legalEvents.last, 'TestEvent1');
    });

    test('Add Listener', () async {
      FutureOr<void> listener(TestEvent1 event) {
        print(event.message);
      }

      emitter = Emitter();
      emitter.addLegalEvent<TestEvent1>();
      emitter.addListener<TestEvent1>(listener);

      expect(emitter.listenerCount(TestEvent1()), 1);
      expect(emitter.listenerCount<TestEvent1>(), 1);
      expect((emitter.rawListeners(TestEvent1())).length, 1);
      expect((emitter.rawListeners<TestEvent1>()).length, 1);
    });
    test('Remove Listener', () async {
      FutureOr<void> listener(TestEvent1 event) {
        print(event.message);
      }

      emitter = Emitter();
      emitter.addLegalEvent<TestEvent1>();
      emitter.on<TestEvent1>(listener);
      expect(emitter.listenerCount<TestEvent1>(), 1);
      emitter.removeListener<TestEvent1>(listener);
      expect(emitter.listenerCount(TestEvent1()), 0);
      expect(emitter.listenerCount<TestEvent1>(), 0);
      expect((emitter.rawListeners(TestEvent1())).isEmpty, isTrue);
      expect((emitter.rawListeners<TestEvent1>()).isEmpty, isTrue);
    });
  });
}
