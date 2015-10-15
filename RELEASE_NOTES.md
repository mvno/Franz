### 1.0.1
* Fixed bug where connecting to a new Kafka cluster without any topics would fail
* Handle OffsetOutOfRange exception correctly
### 1.0.0
* Fixed bug where high level consumer did not handle OffsetOutOfRange error, causing consuming to stop
* Added consumer options to control consumer behaviour, like TcpTimeout, MaxBytes, MinBytes and WaitTime
* Fixed bug where producer wouldn't send messages to topics being autocreate on use
* Handle situation where all brokers are unavailable. This fix throws an exception when this happens, but performing the action again when brokers are available again will work.
* Add support for batching multiple messages in a single produce request
* Support consuming from a subset of partitions
* Support producing to a subset of partitions
* Add overloads to better make it easier to call methods and constructors with many arguments
### 0.0.4
* Initial release
