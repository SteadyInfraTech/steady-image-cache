package peer

// KubernetesWithZone extends Kubernetes discovery with zone-label tracking.
// After every list/watch update it populates a ZoneResolver with the
// topology.kubernetes.io/zone label from each pod's node. The pod struct
// already carries hostIP; nodes are looked up lazily and cached.
//
// Zone information requires the "get"/"list" verb on nodes, which may not be
// granted. When the lookup fails the zone is left empty and ZoneAwareFinder
// degrades gracefully.
//
// Usage: create a KubernetesWithZone, call Run as normal, and wrap the finder
// it produces with ZoneAwareFinder before passing it to the proxy.

// nodePod carries the node name alongside a pod, so that a zone can be resolved.
type nodePod struct {
	pod
	NodeName string `json:"nodeName"`
}

// zonedPodList is a superset of podList that includes the spec.nodeName field.
type zonedPodList struct {
	Metadata struct {
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Items []struct {
		Metadata struct {
			Name              string  `json:"name"`
			ResourceVersion   string  `json:"resourceVersion"`
			DeletionTimestamp *string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Spec struct {
			NodeName string `json:"nodeName"`
		} `json:"spec"`
		Status struct {
			HostIP     string `json:"hostIP"`
			Phase      string `json:"phase"`
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}
