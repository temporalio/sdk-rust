Configuration loading errors now require thread-safe source errors (`Send + Sync`), allowing `ConfigError` to cross async task and Lambda runtime boundaries.
