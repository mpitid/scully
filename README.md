
# Scully

Scully is a simple command line tool for reading from and writing to
[zeromq](http://zeromq.org) sockets, similar to [zmqpp](
https://github.com/zeromq/zmqpp). It uses the [Java bindings](
https://github.com/zeromq/jzmq) for zeromq.

## Building

Running `mvn package` should create a fat jar under `target/fat-scully.jar`.

## Examples

1.  Concatenate two files through a push/pull socket:

    ```bash
    java -jar fat-scully.jar 'tcp://*:4242' -t push < file1 < file2 &
    java -jar fat-scully.jar tcp://localhost:4242 -t pull > merged
    ^C
    ```
