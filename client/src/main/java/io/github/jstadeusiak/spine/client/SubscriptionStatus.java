package io.github.jstadeusiak.spine.client;

/** Transport state; LIVE alone is not a trading freshness or execution readiness guarantee. */
public enum SubscriptionStatus {
    CONNECTING, REPLAYING, LIVE, FAILED, CLOSED
}
