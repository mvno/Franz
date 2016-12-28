### 4.0.0
**Breaking changes**

* Topic has been moved to consumer options, meaning that consumers no longer needs this argument in the constructor.
* Consumers now all take arguments in the same order.
* Brokerseeds are now defined be IEnumerable of EndPoint instead of array
* EndPoint is move into Franz namespace instead of Franz.Internal

**Other changes**

* Support for the group membership features in Kafka. To use these features, make sure to use the GroupConsumer.
* Added support a request to a random broker and to a specific broker specified by broker id.
* IConsumer.SetPosition now takes an IEnumerable instead of an array
* A consumer now exposes a property to get the used BrokerRouter.
* Fixed bug where consumer could not handle a broker being closed while consuming from it
* Retry on all messages defined as retriable in the Kafka protocol documentation, and allow defining retry options in ConsumerOptions. The ErrorRetryBackoff options defines the retry backoff in milliseconds, and the MaximumNumberOfRetriesOnErrors defines the maximum number of retries in a row, 0 means infinite.

### 3.0.1
* Fixed a bug where sending messages without null key, would result in an exception

### 3.0.0
A lot of bugfixes and new features have been included in this release.

* Fixed a bug which would throw an exception when a consumer failed while communicating with a broker. Instead of throwing an exception, we now try to reconnect, and only if this fails throw an exception. The retry interval is defined in the consumer options as ConnectionRetryInterval.
* Support for specifying which partitions to send messages to. This is done by specifying a key along with the messages, and providing a function used to select the partition based on the key. To use round-robin as previously, you should use the new RoundRobinProducer.
* Consumed messages now also contains the offset and partition for which the message were consumed.
* Fixed a bug which would allow you set an offset for a partition, not in the partition whitelist.
* New consumer, ChunkedConsumer, which consume messages in chunks, and won't use as much memory.
* Support for version 2 of the OffsetCommit protocol.
* Fixed bug where OffsetManager for version 1 of the OffsetCommit protocol did not use the default timestamp value. This resulted in not using the expected offset retention time.
* Increment correlation id on each request.
* Rename Set/GetOffsets methods on the consumer to Set/GetPosition, to clearly indicate these methods has nothing with the offsets handled by the OffsetManager to do.
* Support user-defined logging by implementing the ILogger interface and set the logger through LogConfiguration.
* Move partition whitelist into ConsumerOptions.
* Fix bug in producer where an unavailable broker would result in not refreshing the metadata.
* Fix bug where consuming data from a broker going down would result in a infinite loop.
* Experimental support for reading the state and connection to a Kafka cluster through the Zookeeper cluster.
* FSharp.Core is no longer pinned to a specific version
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
