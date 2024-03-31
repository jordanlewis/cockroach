package vectorpb

// IsEmpty returns whether the config contains an index configuration.
func (cfg Config) IsEmpty() bool {
	return cfg.IvfFlat == nil
}
