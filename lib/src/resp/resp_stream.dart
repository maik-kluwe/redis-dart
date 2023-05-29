import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:redis_dart/redis_dart.dart';

class RespStream {
  static final _emptyList = Uint8List.fromList([]);

  final StreamIterator<Uint8List> _input;
  final StreamSink<List<int>> _output;

  Uint8List _buffer = _emptyList;

  RespStream(Socket socket)
      : _input = StreamIterator(socket),
        _output = socket;

  void write(List<Object> arguments) {
    _output.add(RespEncoder.encode(arguments));
  }

  Future<Object?> readValue() async {
    return RespDecoder.decode(this);
  }

  Future<int?> readByte() async {
    final bytes = await readBytes(1);
    return bytes.isNotEmpty ? bytes[0] : null;
  }

  Future<Uint8List> readBytes(int size) async {
    final out = BytesBuilder(copy: false);

    while (size > 0) {
      if (_buffer.isEmpty) {
        if (!(await _input.moveNext())) {
          // Don't attempt to read more data, as there is no more data.
          break;
        }
        _buffer = _input.current;
      }

      if (_buffer.isNotEmpty) {
        if (size < _buffer.length) {
          out.add(Uint8List.sublistView(_buffer, 0, size));
          _buffer = Uint8List.sublistView(_buffer, size);
          break;
        }

        out.add(_buffer);
        size -= _buffer.length;
        _buffer = _emptyList;
      }
    }

    return out.toBytes();
  }

  Future<Uint8List> readLine() async {
    final out = BytesBuilder(copy: false);

    while (true) {
      if (_buffer.isEmpty) {
        if (!(await _input.moveNext())) {
          // Don't attempt to read more data, as there is no more data.
          break;
        }
        _buffer = _input.current;
      }

      if (_buffer.isNotEmpty) {
        final i = _buffer.indexOf('\n'.codeUnitAt(0));
        if (i != -1) {
          out.add(Uint8List.sublistView(_buffer, 0, i + 1));
          _buffer = Uint8List.sublistView(_buffer, i + 1);
          break;
        }

        out.add(_buffer);
        _buffer = _emptyList;
      }
    }

    return out.toBytes();
  }

  Future<dynamic> cancel() async {
    return _input.cancel();
  }

  Future<void> close() async {
    return _output.close().catchError((_) {/* ignore */});
  }
}
