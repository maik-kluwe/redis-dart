import 'dart:async';
import 'dart:collection';
import 'dart:io';

import 'package:redis_dart/redis_dart.dart';

/// Main class to interact with a Redis server.
///
/// Use `RedisClient.connect` to create a connection.
class RedisClient {
  final Socket _socket;
  final RespStream _stream;
  final Queue<Completer<Object?>> _queue;

  bool _closing = false;
  final _closingCompleter = Completer<void>();

  RedisClient._(this._socket)
      : _stream = RespStream(_socket),
        _queue = ListQueue<Completer<Object?>>() {
    scheduleMicrotask(_readInput);
  }

  /// Create a connection to a Redis-server using the [host] & [port] parameters.
  ///
  /// A new connection gets created unsecure by default (non-tls).
  /// To use a secure connection use [secure = true] parameter.
  ///
  /// An [connectionTimelimit] (default: 30 seconds) is used to limit the time to connect to a server.
  static Future<RedisClient> connect({
    required String host,
    required int port,
    Duration connectionTimelimit = const Duration(seconds: 30),
    bool secure = false,
  }) async {
    return _createSocket(
      host: host,
      port: port,
      timeout: connectionTimelimit,
      secure: secure,
    ).then((socket) {
      socket.setOption(SocketOption.tcpNoDelay, true);
      try {
        return RedisClient._(socket);
      } catch (e) {
        rethrow;
      }
    });
  }

  static Future<Socket> _createSocket({
    required String host,
    required int port,
    Duration? timeout,
    bool secure = false,
  }) async {
    if (secure) {
      return SecureSocket.connect(host, port, timeout: timeout);
    }
    return Socket.connect(host, port, timeout: timeout);
  }

  /// Sends a raw command with [arguments] to the server & receives the result as [Object?].
  Future<Object?> sendCommand(List<Object> arguments) async {
    if (_closing) {
      throw RedisClientException('Can not execute operation: connection closed');
    }

    // write commands to server
    _stream.write(arguments);

    // create completer to wait for answer (queue)
    final c = Completer<Object?>();
    _queue.addLast(c);
    return await c.future;
  }

  Future<void> _readInput() async {
    try {
      while (true) {
        Object? value;

        // read from server (wait for input)
        try {
          value = await _stream.readValue();
        } on RedisClientException catch (e, st) {
          await _abort(e, st);
        }

        // handle unexpeceted result from server
        if (_queue.isEmpty) {
          return await _abort(
            RedisClientException('unexpected data from server'),
            StackTrace.current,
          );
        }

        // notify completer with value
        final Completer c = _queue.removeFirst();
        if (value is RedisClientException) {
          c.completeError(value, StackTrace.empty);
        } else {
          c.complete(value);
        }

        // notify close / abort methods that reading from output is finished
        if (_closing && _queue.isEmpty) {
          return _closingCompleter.complete();
        }
      }
    } catch (e, st) {
      await _abort(
        RedisClientException('internal client error: $e'),
        st,
      );
    }
  }

  /// Closes the opened connection.
  ///
  /// Use the [force] parameter to close all pending commands immediately.
  Future<void> close({bool force = false}) async {
    if (!_closing) {
      // send QUIT to server
      try {
        final quit = sendCommand(['QUIT']);
        scheduleMicrotask(() async {
          await quit.catchError((_) => null);
        });
      } catch (_) {
        // ignore
      }
    }
    _closing = true;

    if (force) {
      await _stream.close();

      // Resolve all pending requests
      final pending = _queue.toList(growable: false);
      _queue.clear();

      // complete all pending requests with error that connection got forcibly closed
      final e = RedisClientException('Can not execute operation: connection forcibly closed');
      final st = StackTrace.current;
      for (var c in pending) {
        c.completeError(e, st);
      }
    } else {
      scheduleMicrotask(() async {
        await _stream.close();
      });

      // wait for [_readInput] to finish that the input stream can be closed
      await _closingCompleter.future;
    }

    await _stream.cancel();
  }

  Future<void> _abort(Object e, [StackTrace? st]) async {
    _closing = true;

    // complete all pending requests with info that connection got forcibly closed
    final pending = _queue.toList(growable: false);
    _queue.clear();
    scheduleMicrotask(() {
      for (var c in pending) {
        c.completeError(e, st);
      }
    });

    // force finish _readinput completer
    if (!_closingCompleter.isCompleted) {
      _closingCompleter.complete();
    }

    await _stream.cancel();
  }
}
