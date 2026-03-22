using Xunit;

// Disable parallel test execution so tests don't race for CPU/sockets on CI runners.
// This is more reliable than xunit.runner.json which is ignored by some runners.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
