/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;

import org.junit.Test;

import io.debezium.DebeziumException;

/**
 * Verifies that the LSN bookkeeping of the location phase and the filtering of the replay phase stay consistent
 * when a message is dropped by the decoder (e.g. a decoderbufs message with an unsupported operation).
 */
public class WalPositionLocatorTest {

    private static final Lsn LSN_200 = Lsn.valueOf(200L);
    private static final Lsn LSN_300 = Lsn.valueOf(300L);

    private static ReplicationMessage noopMessage() {
        return new ReplicationMessage.NoopMessage(1L, Instant.EPOCH);
    }

    /**
     * Mirrors a restore of a TIS checkpoint: {@code lastCommitStoredLsn} is null (the Flink CDC offset never
     * persists {@code lsn_proc}), {@code lastEventStoredLsn} is the stored position. Neither the resume position
     * has been located nor filtering enabled yet — callers drive those steps themselves so that the test can
     * interleave {@link WalPositionLocator#recordSeenLsn(Lsn)} at the exact point where the wire-level hook
     * would see messages.
     */
    private static WalPositionLocator locatorAfterRestore() {
        return new WalPositionLocator(null, LSN_200);
    }

    @Test
    public void shouldNotThrowForLsnRecordedAtWireLevel() {
        final WalPositionLocator locator = locatorAfterRestore();

        // an UNKNOWN message is dropped by the decoder: the wire-level hook records its LSN, but resumeFromLsn()
        // never sees it (startStreamingLsn is still null here, i.e. the recording window is still open)
        locator.recordSeenLsn(LSN_300);

        // the next message is dispatched and locates the resume position
        assertTrue(locator.resumeFromLsn(LSN_200, noopMessage()).isPresent());
        locator.enableFiltering();

        // the replay phase re-delivers the dropped message: it must be filtered, not rejected
        assertTrue("an LSN seen on the wire during the location phase must be filtered, not rejected",
                locator.skipMessage(LSN_300));
    }

    @Test
    public void shouldStillThrowForLsnNeverDelivered() {
        final WalPositionLocator locator = locatorAfterRestore();
        assertTrue(locator.resumeFromLsn(LSN_200, noopMessage()).isPresent());
        locator.enableFiltering();
        try {
            locator.skipMessage(LSN_300);
            fail("an LSN that never appeared in the location phase must still be rejected");
        }
        catch (DebeziumException e) {
            assertTrue(e.getMessage().contains("not present among LSNs seen in the location phase"));
        }
    }

    @Test
    public void shouldPassMessagesAtResumeLsn() {
        final WalPositionLocator locator = locatorAfterRestore();
        assertTrue(locator.resumeFromLsn(LSN_200, noopMessage()).isPresent());
        locator.enableFiltering();
        assertFalse("the message at startStreamingLsn must switch the filtering off", locator.skipMessage(LSN_200));
        // filtering is now off, everything passes
        assertFalse(locator.skipMessage(LSN_300));
    }

    @Test
    public void shouldNotRecordAfterReplaySwitchedFilteringOff() {
        final WalPositionLocator locator = locatorAfterRestore();
        assertTrue(locator.resumeFromLsn(LSN_200, noopMessage()).isPresent());
        locator.enableFiltering();
        // replay reaches the resume position: filtering switches off, passMessages flips back to true, lsnSeen cleared
        assertFalse(locator.skipMessage(LSN_200));

        // normal streaming after a restore: recording must be a no-op (startStreamingLsn != null),
        // otherwise lsnSeen would grow unboundedly for the rest of the connector's lifetime
        locator.recordSeenLsn(LSN_300);

        // prove LSN_300 was NOT recorded: with filtering on again it must be treated as never-seen
        locator.enableFiltering();
        try {
            locator.skipMessage(LSN_300);
            fail("LSNs observed after the replay phase must not enter lsnSeen");
        }
        catch (DebeziumException e) {
            // expected
        }
    }

    @Test
    public void shouldIgnoreRecordedLsnWhenNotSearching() {
        // fresh start: no stored position, the locator never filters and must not accumulate LSNs
        final WalPositionLocator locator = new WalPositionLocator();
        locator.recordSeenLsn(LSN_300);
        // passMessages is still true, so nothing is filtered
        assertFalse(locator.skipMessage(LSN_300));
    }
}