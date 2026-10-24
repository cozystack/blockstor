package main

// defaultPlainBindAddress is where the plain-HTTP listener binds unless a
// flag says otherwise. That listener serves the whole API without a client
// certificate, so it stays on loopback: `kubectl port-forward` reaches it
// through the pod's own network namespace, and no other pod can.
const defaultPlainBindAddress = "127.0.0.1:3370"
