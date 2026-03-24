package com.engine.fundatabase.storage;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class Columns {
	private Object key, value;

	@Override
	public String toString() {
		return "[key: " + key + ", value: " + value + "]";
	}
}
