package pep

// The bundle the stage-10 flip makes the default (design §14): every switch
// of the path-authority migration, on. It is defined once here, so the tests
// that measure "the bundle we would ship" (the workload tests' flip arm, the
// DC bed, the DC gate pair) cannot drift from one another or fall a stage
// behind, as the workload tests' arm did when recovery=carrier landed.
// Stage 10 replaces these with the library's own named bundles.

// applyFlipBundleToClient sets every migration switch of a client to the
// default-to-be bundle.
func applyFlipBundleToClient(c *ClientConfig) {
	c.CCAPI, c.Sched, c.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
	c.CodedRetry, c.Memory, c.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
	c.Recovery, c.Topology = RecoveryCarrier, TopologySingle
}

// applyFlipBundleToServer is applyFlipBundleToClient for a gateway.
func applyFlipBundleToServer(s *ServerConfig) {
	s.CCAPI, s.Sched, s.CongestionScope = CCAPIV2, SchedClass, CongestionScopePath
	s.CodedRetry, s.Memory, s.Classifier = CodedRetrySticky, MemoryOwned, ClassifierPhased
	s.Recovery, s.GatewayAdmission = RecoveryCarrier, GatewayAdmissionOwned
}

// applyLegacyBundleToClient sets every migration switch of a client off,
// explicitly, so a control stays the legacy bundle whatever the defaults are.
func applyLegacyBundleToClient(c *ClientConfig) {
	c.CCAPI, c.Sched, c.CongestionScope = CCAPIV1, SchedLegacy, CongestionScopeConnection
	c.CodedRetry, c.Memory, c.Classifier = CodedRetryLegacy, MemoryLegacy, ClassifierLegacy
	c.Recovery, c.Topology = RecoveryFlow, TopologyIsolated
}

// applyLegacyBundleToServer is applyLegacyBundleToClient for a gateway.
func applyLegacyBundleToServer(s *ServerConfig) {
	s.CCAPI, s.Sched, s.CongestionScope = CCAPIV1, SchedLegacy, CongestionScopeConnection
	s.CodedRetry, s.Memory, s.Classifier = CodedRetryLegacy, MemoryLegacy, ClassifierLegacy
	s.Recovery, s.GatewayAdmission = RecoveryFlow, GatewayAdmissionLegacy
}
