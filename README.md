### Redis client for Dart

See [example](example/) directory for examples and usage

### Roadmap
* [ ] Command abstractions in RedisClient (like `client.set('key',...)` or `client.get('key'...)`)
* [ ] Pub / Sub stream subscriptions
* [ ] SSL for Secure connections

### Usage

```dart
final client = await RedisClient.connect(
  ost: 'localhost',
  port: 6379,
);
```

### Send client commands
```dart
// Send simple string values
final result = client.sendCommand(['SET', 'key1', 'value1']);
print(result); // prints: 'OK'

// Send simple integer
final result = client.sendCommand(['SET', 'key2', 1]);
print(result); // prints: 'OK'

// Increment value
final result = client.sendCommand(['INCR', 'key2']);
print(result); // prints: '2'
```