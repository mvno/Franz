Franz
=====

<div class="row">
  <div class="span1"></div>
  <div class="span6">
    <div class="well well-small" id="nuget">
      The Franz library can be <a href="https://nuget.org/packages/Franz">installed from NuGet</a>:
      <pre>PM> Install-Package Franz</pre>
    </div>
  </div>
  <div class="span1"></div>
</div>

Documentation
-------------

Franz is a .NET client for Kafka implemented in F#. It supports Kafka version 0.8.2.1 and below. At the moment the client is synchronous, as this makes it easier to debug and determine if problems are related to network of client issues.
In the future it may support asynchronous sending and receiving, but in the uses cases we need at the moment, it doesn't affect performance that much.

Example
-------

Producer:

    [lang=csharp]
    using Franz.Highlevel
	
    // Key is allowed to be null
    var message = new Message("testValue", "testKey");

    var roundRobinProducer = new RoundRobinProducer(new[] { new EndPoint("localhost", 9092) });
    roundRobinProducer.SendMessage("testTopic", message);

    // This producer allows you to control which partition messages and sent to
    var producer = new Producer(new[] { new EndPoint("localhost", 9092) }, (topic, key) => 1);
    producer.SendMessage("testTopic", message);

Consumer:

    [lang=csharp]
    using Franz.Highlevel
	
    var cts = new CancellationTokenSource();
    var token = cts.Token;
    var endPoints = new[] { new EndPoint("localhost", 9092), new EndPoint("localhost", 9093) };

    var consumer = new Consumer(endPoints, "testTopic")
    foreach (var message = consumer.Consume())
    {
      /// Process message
    }

    // Chunked consumers uses much less memory, but receives messages slower, and isn't as easy to use
    var chunkedConsumer = new ChunkedConsumer(endPoints, "testTopic");
    while(true)
    {
      foreach (var message in chunkedConsumer.Consume(token))
      {
        // Handle message
      }
    }

Samples & documentation
-----------------------

The library comes with comprehensible documentation. 

 * [API Reference](reference/index.html) contains automatically generated documentation for all types, modules
   and functions in the library.
 
Contributing and copyright
--------------------------

The project is hosted on [GitHub][gh] where you can [report issues][issues], fork 
the project and submit pull requests. If you're adding a new public API, please also 
consider adding [samples][content] that can be turned into a documentation. You might
also want to read the [library design notes][readme] to understand how it works.

  [content]: https://github.com/mvno/Franz/tree/master/docs/content
  [gh]: https://github.com/mvno/Franz
  [issues]: https://github.com/mvno/Franz/issues
  [readme]: https://github.com/mvno/Franz/blob/master/README.md
  [license]: https://github.com/mvno/Franz/blob/master/LICENSE.txt
