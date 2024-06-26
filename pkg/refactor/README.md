# Refactor

==========================

The Refactor is driven by the go-client informer, which `List/Watch` operations feed into the `DeltaFIFO`.

Another process pops events from the `DeltaFIFO` and then:

1. Updates the event in a local cache
2. Sends the event to the transport (similar to the role of `ResourceEventHandler` in a shared informer)

It also provides a Resync capability. This leverages the local cache and `DeltaFIFO` to replay resources from the local cache and push them back into the `DeltaFIFO`. That's what DeltaFIFO.Resync() does.