package com.labs.watermelon.gulp;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FileProcessorThread implements Runnable {

	private InputQueue in;
	private OutputQueue out;
	private String keyPattern;
	private String keyRegex;
	private int readChunkSize;
	private String carryOverLine = "";
	private String key;
	private String fileId;
	private int keyStartIndex;
	private int keyEndIndex;
	private boolean noRegex;

	public FileProcessorThread(InputQueue in, OutputQueue out,
			String keyPattern, String keyRegex, int keyStartIndex,
			int keyEndIndex, boolean noRegex, int readChunkSize, String fileId) {
		this.in = in;
		this.out = out;
		this.keyPattern = keyPattern;
		this.keyRegex = keyRegex;
		this.keyStartIndex = keyStartIndex;
		this.keyEndIndex = keyEndIndex;
		this.noRegex = noRegex;
		this.readChunkSize = readChunkSize;
		this.fileId = fileId;
	}

	public void run() {
		long seqNo = 0;
		while ((!in.getStatus().equals("done")) || (in.getInQ().size() != 0)) {
			if (in.getInQ().size() == 0) {
				synchronized (in) {
					try {
						in.wait();
					} catch (InterruptedException e) {
						System.err
								.println("Exception in processing while waiting");
					}
				}
			}
			String chunk = in.getInQ().remove();
			String newChunk = carryOverLine + chunk;
			int carryOverLength = carryOverLine.length();
			carryOverLine = "";
			if ((chunk.length() == readChunkSize)
					&& (chunk.charAt(readChunkSize - 1) != '\n')
					&& (chunk.charAt(readChunkSize - 1) != '\r')) {
				int lastNewline = chunk.lastIndexOf('\n');
				if (lastNewline == -1)
					lastNewline = chunk.lastIndexOf('\r');
				if (lastNewline != -1) {
					carryOverLine = chunk.substring(lastNewline + 1);
					newChunk = newChunk.substring(0, lastNewline
							+ carryOverLength);
				}
			}

			String lines[] = newChunk.split("\\r?\\n");
			Map<String, String> kv = new TreeMap<String, String>();

			if (!noRegex) {
				Pattern pattern = Pattern.compile(keyRegex);
				Matcher matcher = null;
				DateTimeFormatter fmtIn = DateTimeFormat.forPattern(keyPattern);
				DateTimeFormatter fmtOut = DateTimeFormat
						.forPattern("yyyy/MM/dd#HH:mm:ss.S");
				DateTime dt = null;
				for (String line : lines) {
					matcher = pattern.matcher(line);
					if (matcher.find()) {
						// change key pattern
						dt = fmtIn.parseDateTime(line.substring(
								matcher.start(), matcher.end()));
						key = fileId + "#" + fmtOut.print(dt) + "#";
						kv.put(key + seqNo++, line);
					} else {
						if ((!line.equals("")) && (key != null)) {
							kv.put(key + seqNo++, line);
						}
					}
				}
			} else {
				DateTimeFormatter fmtIn = DateTimeFormat.forPattern(keyPattern);
				DateTimeFormatter fmtOut = DateTimeFormat
						.forPattern("yyyy/MM/dd#HH:mm:ss.SSS");
				DateTime dt = null;
				for (String line : lines) {
					dt = fmtIn.parseDateTime(line.substring(keyStartIndex,
							keyEndIndex));
					key = fileId + "#" + fmtOut.print(dt) + "#";
					kv.put(key + seqNo++, line);
				}
			}

			synchronized (out) {
				try {
					out.getOutQ().put(kv);
				} catch (InterruptedException e) {
					System.err
							.println("Interupted Exception while adding map to outQ");
				}
				out.notify();
			}
		}
		out.setStatus("done");
	}

}
