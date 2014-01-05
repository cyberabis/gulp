package com.labs.watermelon.gulp;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class InputQueue {

	private String status = "";
	private BlockingQueue<String> inQ = new ArrayBlockingQueue<String>(500);

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public BlockingQueue<String> getInQ() {
		return inQ;
	}

	public void setInQ(BlockingQueue<String> inQ) {
		this.inQ = inQ;
	}

}