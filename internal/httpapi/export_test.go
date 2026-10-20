package httpapi

// Classify exposes the error-to-status mapping to the package's external
// tests, which is the only thing outside this package that needs it.
var Classify = classify
