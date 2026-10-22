Workers no longer leak memory for every activity task they run. Each task's cancellation handle was previously kept until the worker exited.
