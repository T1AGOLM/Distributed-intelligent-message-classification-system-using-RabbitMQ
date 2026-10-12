package com.distributed.consumer;

/**
 * Message consumed from the queue (same JSON shape the generator publishes):
 * {@code id} (UUID), {@code timestamp} (ISO-8601), {@code type} and
 * {@code content}.
 */
public record Message(String id, String timestamp, String type, String content) {
}
