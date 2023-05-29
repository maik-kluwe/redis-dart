class RedisClientException implements Exception {
  final String message;

  RedisClientException(this.message);

  @override
  String toString() => '$runtimeType: $message';
}

class RedisCommandException implements Exception {
  final String type;
  final String message;

  RedisCommandException(this.type, this.message);

  @override
  String toString() => '$runtimeType: $type $message';
}
