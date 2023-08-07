package refactor

import (
	"fmt"
	"math/rand"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	"k8s.io/utils/trace"
)

// watchList establishes a stream to get a consistent snapshot of data
// from the server as described in https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#proposal
//
// case 1: start at Most Recent (RV="", ResourceVersionMatch=ResourceVersionMatchNotOlderThan)
// Establishes a consistent stream with the server.
// That means the returned data is consistent, as if, served directly from etcd via a quorum read.
// It begins with synthetic "Added" events of all resources up to the most recent ResourceVersion.
// It ends with a synthetic "Bookmark" event containing the most recent ResourceVersion.
// After receiving a "Bookmark" event the reflector is considered to be synchronized.
// It replaces its internal store with the collected items and
// reuses the current watch requests for getting further events.
//
// case 2: start at Exact (RV>"0", ResourceVersionMatch=ResourceVersionMatchNotOlderThan)
// Establishes a stream with the server at the provided resource version.
// To establish the initial state the server begins with synthetic "Added" events.
// It ends with a synthetic "Bookmark" event containing the provided or newer resource version.
// After receiving a "Bookmark" event the reflector is considered to be synchronized.
// It replaces its internal store with the collected items and
// reuses the current watch requests for getting further events.
func (r *Reflector) watchList(stopCh <-chan struct{}, clusterName string) (watch.Interface, error) {
	var w watch.Interface
	var err error
	var temporaryStore Store
	var resourceVersion string
	// TODO(#115478): see if this function could be turned
	//  into a method and see if error handling
	//  could be unified with the r.watch method
	isErrorRetriableWithSideEffectsFn := func(err error) bool {
		if canRetry := isWatchErrorRetriable(err); canRetry {
			klog.V(2).Infof("%s: watch-list of %v returned %v - backing off", r.name, r.typeDescription, err)
			<-r.initConnBackoffManager.Backoff().C()
			return true
		}
		if isExpiredError(err) || isTooLargeResourceVersionError(err) {
			// we tried to re-establish a watch request but the provided RV
			// has either expired or it is greater than the server knows about.
			// In that case we reset the RV and
			// try to get a consistent snapshot from the watch cache (case 1)
			r.setIsLastSyncResourceVersionUnavailable(true)
			return true
		}
		return false
	}

	initTrace := trace.New("Reflector WatchList", trace.Field{Key: "name", Value: r.name})
	defer initTrace.LogIfLong(10 * time.Second)
	for {
		select {
		case <-stopCh:
			return nil, nil
		default:
		}

		resourceVersion = ""
		// lastKnownRV := r.rewatchResourceVersion()
		lastKnownRV := r.rewatchResourceVersionFromCluster(clusterName)

		temporaryStore = cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
		// TODO(#115478): large "list", slow clients, slow network, p&f
		//  might slow down streaming and eventually fail.
		//  maybe in such a case we should retry with an increased timeout?
		timeoutSeconds := int64(minWatchTimeout.Seconds() * (rand.Float64() + 1.0))
		options := metav1.ListOptions{
			ResourceVersion:      lastKnownRV,
			AllowWatchBookmarks:  true,
			SendInitialEvents:    pointer.Bool(true),
			ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
			TimeoutSeconds:       &timeoutSeconds,
		}
		start := r.clock.Now()

		w, err = r.listerWatcher.Watch(options)
		if err != nil {
			if isErrorRetriableWithSideEffectsFn(err) {
				continue
			}
			return nil, err
		}
		bookmarkReceived := pointer.Bool(false)
		err = watchHandler(start, w, temporaryStore, r.expectedType, r.expectedGVK, r.name, r.typeDescription,
			func(rv string) { resourceVersion = rv },
			bookmarkReceived,
			r.clock, make(chan error), stopCh)
		if err != nil {
			w.Stop() // stop and retry with clean state
			if err == errorStopRequested {
				return nil, nil
			}
			if isErrorRetriableWithSideEffectsFn(err) {
				continue
			}
			return nil, err
		}
		if *bookmarkReceived {
			break
		}
	}
	// We successfully got initial state from watch-list confirmed by the
	// "k8s.io/initial-events-end" bookmark.
	initTrace.Step("Objects streamed", trace.Field{Key: "count", Value: len(temporaryStore.List())})
	r.setIsLastSyncResourceVersionUnavailable(false)
	if err = r.store.Replace(temporaryStore.List(), resourceVersion); err != nil {
		return nil, fmt.Errorf("unable to sync watch-list result: %v", err)
	}
	initTrace.Step("SyncWith done")
	r.setLastSyncResourceVersion(resourceVersion)

	return w, nil
}

// rewatchResourceVersion determines the resource version the reflector should start streaming from.
func (r *Reflector) rewatchResourceVersionFromCluster(clusterName string) string {
	rv, ok := r.lastSyncResourceVersions[clusterName]
	if !ok {
		return ""
	}
	rv.mutex.RLock()
	defer rv.mutex.RUnlock()

	if rv.isUnavailable {
		// initial stream should return data at the most recent resource version.
		// the returned data must be consistent i.e. as if served from etcd via a quorum read
		return ""
	}
	return rv.version
}
