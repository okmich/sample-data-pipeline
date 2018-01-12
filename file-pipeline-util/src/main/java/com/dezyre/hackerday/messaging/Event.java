package com.dezyre.hackerday.messaging;

public class Event {

	private String code;
	private String fileName;
	private String payload;
	private long timestamp;

	public Event(String code, String fileName, String payload) {
		this.fileName = fileName;
		this.payload = payload;
		this.code = code;
		this.timestamp = System.currentTimeMillis();
	}

	public String getCode() {
		return code;
	}

	public String getFileName() {
		return fileName;
	}

	public String getPayload() {
		return payload;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return String.format("%s|%s|%s|%d", this.code, this.fileName,
				this.payload, this.timestamp);
	}

	public static Event fromString(String arg) {
		String[] parts = arg.split("\\|");

		Event event = new Event(parts[0], parts[1], parts[2]);
		event.timestamp = Long.parseLong(parts[3]);

		return event;
	}

}
