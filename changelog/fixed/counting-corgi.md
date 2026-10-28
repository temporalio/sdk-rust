Resource-based worker tuning now reads CPU usage and quotas from standard cgroup v1 mounts, preventing host CPU measurements from understating container load when cgroup v2 files are unavailable.
