package com.engine.fundatabase.storage;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class Columns {
	private Object key, value;

	
	public Columns(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	@Override
	public String toString() {
		return "[key: " + key + ", value: " + value + "]";
	}

	
}
