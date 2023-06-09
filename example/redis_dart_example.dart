import 'package:redis_dart/redis_dart.dart';

void main() async {
  // create connection
  final client = await RedisClient.connect(
    host: 'localhost',
    port: 6379,
  );
  print('connected');

  // insert some keys
  print(client.sendCommandRaw(['SET', 'key1', 'value1']));
  print(client.sendCommandRaw(['SET', 'key2', 2]));

  // get value of key2
  print(client.sendCommandRaw(['GET', 'key2'])); // prints 2

  // close connection
  await client.close();
}
