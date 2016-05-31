### 3.0.0
* Fix bug which would throw exception when consumer got exceptions while communicating with broker. This is done by trying to reconnect when consuming fails. The retry interval is defined using the ConnectionRetryInterval property in the consumer options.
* Allow selecting specific partitions when producing messages, using a specified key. To use round robin as previously, please use the RoundRobinProducer. When key is passed this key is passed along to Kafak message
* Consuming messages with metadata no also includes the partition for which the message is consumed
* Do not allow setting offset not in partition whitelist
* New consumer, ChunkedConsumer, which consume messages in chunks, and won't use as much memory
* Support for version 2 of the OffsetCommit protocol
* Fixed bug where OffsetManager for version 1 of the OffsetCommit protocol did not use the default timestamp value. This resulted in not using the expected offset retention time
* Increment correlation id on each request
* Replace Consumer.Consume method with Consumer.ConsumeWithMetdata
* Rename Set/GetOffsets methods on the consumer to Set/GetPosition, to clearly indicate these methods has nothing with the offsets handled by the OffsetManager
* Support user-defined logging by implementing the ILogger interface and set the logger through LogConfiguration
* Move partition whitelist into ConsumerOptions
* Fix bug in producer where an unavailable broker would result in not refreshing the metadata
* Fix bug where consuming data from a broker going down would result in a infinite loop
* Support for connecting to the cluster through the Zookeeper cluster. There is currently no support for handling new topics created while the consumer is running.
### 2.1.0
* Handle situtation where fetching big messagesets could result in a infinite loop
* Fix memory leak
* Allow reusing BrokerRouter to minimize number of TCP connections to the cluster
* Implement IDisposable on consumer, producer, offsetmanagers and brokerrouter to allow closing TCP connections
* Add support for Snappy and GZip compression
### 2.0.1
* Added missing getter and setter to offset storage
### 2.0.0
* Fixed bug where connecting to a new Kafka cluster without any topics would fail
* Handle OffsetOutOfRange exception correctly
* Try to fix race condition where TcpClient can be null and stop consuming
* Move ConsumerOffsetManager creation into Consumer, this is a breaking change
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
