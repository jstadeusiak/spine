package io.github.jstadeusiak.spine.protocol;

/** Provisioned, non-reused application semantics and configuration. Constructed before readiness. */
public record ApplicationIdentity(int id, int version, int configurationId) {
    public static final ApplicationIdentity PASS_THROUGH = new ApplicationIdentity(1, 1, 1);

    public ApplicationIdentity {
        if (id == 0 || id == -1 || version <= 0 || configurationId == 0 || configurationId == -1) {
            throw new IllegalArgumentException("invalid application identity");
        }
    }

    public boolean matches(final long applicationId, final long applicationVersion, final long configuration) {
        return applicationId == Integer.toUnsignedLong(id) && applicationVersion == version
            && configuration == Integer.toUnsignedLong(configurationId);
    }
}
