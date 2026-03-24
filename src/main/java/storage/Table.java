package storage;

import java.util.Hashtable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;


@AllArgsConstructor
@Getter
@Setter
public class Table {

	private String name, pkColumn;
	private Row row;
	private int size;
	private Hashtable<String, String> colNameType, colNameMin, colNameMax;
	private String primaryKeyType;
	
	
	
	
}
