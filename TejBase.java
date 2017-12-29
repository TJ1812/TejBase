import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author Tej Patel
 * @version 1.8-STING
 */
public class TejBase {
	static String prompt = "tejsql> ";
	static String version = "v1.8-STING";
	static String copyright = "©2017 Tej Patel";
	static RandomAccessFile tejBaseTables;
	static RandomAccessFile tejBaseColumns;
	static boolean isExit = false;
	static HashMap<String, Integer> dataCodes;
	static HashMap<String, Integer> sizeOfDataCodes;
	/*
	 * Page size for all files is 512 bytes by default.
	 */
	static long pageSize = 512;

	/*
	 * The Scanner class is used to collect user commands from the prompt There are
	 * many ways to do this. This is just one.
	 *
	 * Each time the semicolon (;) delimiter is entered, the userCommand String is
	 * re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	/**
	 * *********************************************************************** Main
	 * method
	 * 
	 */
	public static void main(String[] args) throws Exception {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = "";
		tejBaseTables = new RandomAccessFile(new File("data/catalog/tejbase_tables.tbl"), "rw");
		tejBaseColumns = new RandomAccessFile(new File("data/catalog/tejbase_columns.tbl"), "rw");
		pageSize *= 4;
		dataCodes = new HashMap<String, Integer>();
		dataCodes.put("tinyint", 0x04);
		dataCodes.put("smallint", 0x05);
		dataCodes.put("int", 0x06);
		dataCodes.put("bigint", 0x07);
		dataCodes.put("real", 0x08);
		dataCodes.put("double", 0x09);
		dataCodes.put("datetime", 0x0A);
		dataCodes.put("date", 0x0B);
		dataCodes.put("text", 0x0C);

		sizeOfDataCodes = new HashMap<String, Integer>();
		sizeOfDataCodes.put("tinyint", 1);
		sizeOfDataCodes.put("smallint", 2);
		sizeOfDataCodes.put("int", 4);
		sizeOfDataCodes.put("bigint", 8);
		sizeOfDataCodes.put("real", 4);
		sizeOfDataCodes.put("double", 8);
		sizeOfDataCodes.put("datetime", 8);
		sizeOfDataCodes.put("date", 8);

		while (!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Saving changes....");

		/* Function to save the files goes here. */
		tejBaseColumns.close();
		tejBaseTables.close();
		System.out.println("Exited Successfully");
	}

	/**
	 * ***********************************************************************
	 * Method definitions
	 */

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to TejBaseLite");
		System.out.println("TejBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num
	 *         times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*", 80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
		System.out.println("\tVERSION;                                         Show the program version.");
		System.out.println("\tHELP;                                            Show this help information");
		System.out.println("\tEXIT;                                            Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*", 80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("TejBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) throws IOException {

		/*
		 * commandTokens is an array of Strings that contains one token per array
		 * element The first token can be used to determine the type of command The
		 * other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		/*
		 * This switch handles a very small list of hardcoded commands of known syntax.
		 * You will want to rewrite this method to interpret more complex commands.
		 */
		switch (commandTokens.get(0)) {
		case "select":
			parseQueryString(userCommand);
			break;
		case "show":
			parseShowString(userCommand);
			break;
		case "insert":
			parseInsertString(userCommand);
			break;
		case "delete":
			parseDeleteString(userCommand);
			break;
		case "drop":
			System.out.println("STUB: Calling your method to drop items");
			dropTable(userCommand);
			break;
		case "create":
			parseCreateString(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	private static void parseDeleteString(String userCommand) throws IOException {
		System.out.println("STUB: Calling parseDeleteString(String s) to process queries");
		System.out.println("Parsing the string:\"" + userCommand + "\"");
		ArrayList<String> deleteTableTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		String tableFileName = deleteTableTokens.get(deleteTableTokens.indexOf("from") + 1);

		// check if table exists
		boolean tableExists = false;
		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			seekPtr += 2;
			recLocation += 6;
			tejBaseTables.seek(recLocation);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 1);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			String tableName = new String(tableNameBytes);
			if (tableName.equals(tableFileName)) {
				tableExists = true;
				break;
			}
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}
		if (tableExists) {
			ArrayList<String> colList = new ArrayList<String>();
			int index = 1;
			while (!deleteTableTokens.get(index).equals("from")) {
				if (!deleteTableTokens.get(index).equals(",")) {
					colList.add(deleteTableTokens.get(index));
				}
				index++;
			}
			String compareCol = deleteTableTokens.get(deleteTableTokens.indexOf("where") + 1);
			String operator = deleteTableTokens.get(deleteTableTokens.indexOf("where") + 2);
			String compareValue = deleteTableTokens.get(deleteTableTokens.indexOf("where") + 3);
			ArrayList<String> dataTypes = new ArrayList<String>();
			int ordinalPosCompareCol = 0;
			ArrayList<Integer> ordinals = new ArrayList<Integer>();
			tejBaseColumns.seek(1);
			int noRecords = tejBaseColumns.readByte();
			int recordPtr = 8;
			for (int i = 0; i < noRecords; i++) {
				tejBaseColumns.seek(recordPtr);
				recordPtr += 2;
				recLocation = tejBaseColumns.readShort();
				tejBaseColumns.seek(recLocation + 12);
				byte[] tblName = new byte[tableFileName.length()];
				tejBaseColumns.read(tblName, 0, tableFileName.length());
				if (tableFileName.equals(new String(tblName))) {
					tejBaseColumns.seek(recLocation + 8);
					int colNameLength = tejBaseColumns.readByte() - 0x0c;
					int dataTypeLength = tejBaseColumns.readByte() - 0x0c;
					tejBaseColumns.seek(recLocation + 12 + tableFileName.length());
					byte[] clName = new byte[colNameLength];
					tejBaseColumns.read(clName, 0, colNameLength);
					byte[] dt = new byte[dataTypeLength];
					tejBaseColumns.read(dt, 0, dataTypeLength);
					dataTypes.add(new String(dt));
					int op = tejBaseColumns.readByte();
					if (colList.contains(new String(clName)) || colList.contains("*")) {
						ordinals.add(op);
					}
					if (new String(clName).equals(compareCol)) {
						ordinalPosCompareCol = op;
					}
				}
			}
			Collections.sort(ordinals);
			RandomAccessFile tableName = new RandomAccessFile(new File("data/user_data/" + tableFileName + ".tbl"),
					"rw");
			tableName.seek(1);
			noRecords = tableName.readUnsignedByte();
			recordPtr = 8;
			tableName.seek(recordPtr);
			recLocation = tableName.readShort();
			int recCount = 0;
			while (recLocation != 0) {
				recCount++;
				boolean show = false;
				ArrayList<String> colValues = new ArrayList<String>();
				ArrayList<Integer> textCodeValues = new ArrayList<Integer>();
				recordPtr += 2;
				tableName.seek(recLocation + 7);
				for (int j = 0; j < dataTypes.size(); j++) {
					textCodeValues.add(tableName.readUnsignedByte());
				}
				for (int j = 0; (j < dataTypes.size()); j++) {
					switch (dataTypes.get(j)) {
					case "tinyint":
						Byte b = new Byte(tableName.readByte());
						colValues.add(b.toString());
						break;
					case "smallint":
						Integer si = new Integer(tableName.readShort());
						colValues.add(si.toString());
						break;
					case "int":
						Integer i = new Integer(tableName.readInt());
						colValues.add(i.toString());
						break;
					case "bigint":
						Long bi = new Long(tableName.readLong());
						colValues.add(bi.toString());
						break;
					case "real":
						Float r = new Float(tableName.readFloat());
						colValues.add(r.toString());
						break;
					case "double":
						Double d = new Double(tableName.readDouble());
						colValues.add(d.toString());
						break;
					case "datetime":
						Double dt = new Double(tableName.readDouble());
						colValues.add(dt.toString());
						break;
					case "date":
						Double ddt = new Double(tableName.readDouble());
						colValues.add(ddt.toString());
						break;
					case "text":
						byte[] txtVal = new byte[textCodeValues.get(j) - 0x0c];
						tableName.read(txtVal, 0, txtVal.length);
						colValues.add(new String(txtVal));
						break;
					default:
						break;
					}
					if (j == ordinalPosCompareCol) {
						switch (operator) {
						case "=":
							if (colValues.get(j).equals(compareValue)) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "!=":
							if (!colValues.get(j).equals(compareValue)) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case ">":
							if (Double.parseDouble(colValues.get(j)) > Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "<":
							if (Double.parseDouble(colValues.get(j)) < Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case ">=":
							if (Double.parseDouble(colValues.get(j)) >= Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "<=":
							if (Double.parseDouble(colValues.get(j)) <= Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;

						default:
							break;
						}
					}
				}
				if (show) {
					tableName.seek(recLocation);
					int payload = tableName.readShort();
					tableName.seek(recLocation);
					tableName.writeShort(0);
					tableName.writeInt(0);
					for (int j = 0; j <= payload; j++)
						tableName.writeByte(0);
					deleteNShiftBytes(tableName, noRecords, recCount);
					recordPtr -= 2;
					recCount -= 1;
					noRecords -= 1;
				}
				tableName.seek(recordPtr);
				recLocation = tableName.readShort();
			}
			System.out.println("Successfully deleted");
			tableName.close();
		} else {
			System.out.println("Table does not exist");
		}
	}

	private static void parseShowString(String showTablesString) throws IOException {
		System.out.println("STUB: Calling your method to show tables");
		System.out.println("Parsing the string:\"" + showTablesString + "\"");

		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			seekPtr += 2;
			recLocation += 6;
			tejBaseTables.seek(recLocation);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 1);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			System.out.println(new String(tableNameBytes));
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}
	}

	private static void parseInsertString(String insertRecordString) throws IOException {
		System.out.println("STUB: Calling your method to insert into a table");
		System.out.println("Parsing the string:\"" + insertRecordString + "\"");
		ArrayList<String> insertRecordTokens = new ArrayList<String>(Arrays.asList(insertRecordString.split(" ")));

		String tableFileName = insertRecordTokens.get(2);

		// check if table already exists
		boolean tableExists = false;
		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			seekPtr += 2;
			recLocation += 6;
			tejBaseTables.seek(recLocation);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 1);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			String tableName = new String(tableNameBytes);
			if (tableName.equals(tableFileName)) {
				tableExists = true;
				break;
			}
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}

		// get information like column name , data type and nullable
		if (tableExists) {
			ArrayList<String> colNames = new ArrayList<String>();
			ArrayList<String> dataTypes = new ArrayList<String>();
			ArrayList<String> isNullable = new ArrayList<String>();
			tejBaseColumns.seek(1);
			int noRecords = tejBaseColumns.readByte();
			int recordPtr = 8;
			for (int i = 0; i < noRecords; i++) {
				tejBaseColumns.seek(recordPtr);
				recordPtr += 2;
				recLocation = tejBaseColumns.readShort();
				tejBaseColumns.seek(recLocation + 12);
				byte[] tblName = new byte[tableFileName.length()];
				tejBaseColumns.read(tblName, 0, tableFileName.length());
				if (tableFileName.equals(new String(tblName))) {
					tejBaseColumns.seek(recLocation + 8);
					int colNameLength = tejBaseColumns.readByte() - 0x0c;
					int dataTypeLength = tejBaseColumns.readByte() - 0x0c;
					tejBaseColumns.seek(recLocation + 12 + tableFileName.length());
					byte[] clName = new byte[colNameLength];
					tejBaseColumns.read(clName, 0, colNameLength);
					colNames.add(new String(clName));
					byte[] dt = new byte[dataTypeLength];
					tejBaseColumns.read(dt, 0, dataTypeLength);
					dataTypes.add(new String(dt));
					tejBaseColumns.readByte();
					byte[] c = new byte[1];
					tejBaseColumns.read(c, 0, 1);
					if (new String(c).equals("Y")) {
						isNullable.add("YES");
					} else {
						isNullable.add("NO");
					}
				}
			}
			int index = 3 + (2 * colNames.size() - 1) + 4;
			ArrayList<String> values = new ArrayList<String>();
			while (index < insertRecordTokens.size()) {
				values.add(insertRecordTokens.get(index));
				index += 2;
			}

			boolean consistency = true;
			for (int j = 0; j < isNullable.size(); j++) {
				if (isNullable.get(j).equals("NO")) {
					if (values.get(j).equals("")) {
						consistency = false;
						break;
					}
				}
			}

			// now insert the values into file
			if (consistency) {
				RandomAccessFile tableName = new RandomAccessFile(new File("data/user_data/" + tableFileName + ".tbl"),
						"rw");
				tableName.seek(1);
				noRecords = tableName.readByte();

				// add record if id doesnt exist
				int startingPos = tableName.readShort();
				int payload = values.size();
				for (int j = 0; j < values.size(); j++) {
					String dataType = dataTypes.get(j);
					if (sizeOfDataCodes.containsKey(dataType)) {
						payload += sizeOfDataCodes.get(dataType);
					} else {
						payload += values.get(j).length();
					}
				}
				int startingPosForNewEntry = startingPos - (payload) - 7;
				tableName.seek(startingPosForNewEntry);
				tableName.writeShort(payload);
				tableName.writeInt(noRecords + 1);
				tableName.writeByte(values.size() - 1);
				for (int j = 0; j < values.size(); j++) {
					String dataType = dataTypes.get(j);
					if (sizeOfDataCodes.containsKey(dataType)) {
						if (values.get(j).equals("")) {
							if (sizeOfDataCodes.get(dataType) == 1) {
								tableName.writeByte(0x00);
							} else if (sizeOfDataCodes.get(dataType) == 2) {
								tableName.writeByte(0x01);
							} else if (sizeOfDataCodes.get(dataType) == 4) {
								tableName.writeByte(0x02);
							} else if (sizeOfDataCodes.get(dataType) == 8) {
								tableName.writeByte(0x03);
							}
						} else {
							tableName.writeByte(dataCodes.get(dataType));
						}
					} else {
						if (values.get(j).equals("")) {
							tableName.writeByte(0x0c);
						} else {
							tableName.writeByte(0x0c + values.get(j).length());
						}
					}
				}
				for (int j = 0; j < values.size(); j++) {
					switch (dataTypes.get(j)) {
					case "tinyint":
						tableName.writeByte(Integer.parseInt(values.get(j)));
						break;
					case "smallint":
						tableName.writeShort(Integer.parseInt(values.get(j)));
						break;
					case "int":
						tableName.writeInt(Integer.parseInt(values.get(j)));
						break;
					case "bigint":
						tableName.writeLong(Long.parseLong(values.get(j)));
						break;
					case "real":
						tableName.writeFloat(Float.parseFloat(values.get(j)));
						break;
					case "double":
						tableName.writeDouble(Double.parseDouble(values.get(j)));
						break;
					case "datetime":
						tableName.writeDouble(Double.parseDouble(values.get(j)));
						break;
					case "date":
						tableName.writeDouble(Double.parseDouble(values.get(j)));
						break;
					case "text":
						tableName.writeBytes(values.get(j));
						break;
					default:
						break;
					}
				}
				tableName.seek(1);
				int temp = noRecords * 2 + 8;
				tableName.writeByte(noRecords + 1);
				tableName.writeShort(startingPosForNewEntry);
				tableName.seek(temp);
				tableName.writeShort(startingPosForNewEntry);

				tableName.close();
				System.out.println("Successfully inserted row");
			}
		} else {
			System.out.println("Table does not exist, create table first");
		}
	}

	/**
	 * Stub method for dropping tables
	 * 
	 * @param dropTableString
	 *            is a String of the user input
	 * @throws IOException
	 */
	public static void dropTable(String dropTableString) throws IOException {
		System.out.println("STUB: Calling dropTable(String s) to drop a table");
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		ArrayList<String> dropTableTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
		String tableFileName = dropTableTokens.get(2);
		boolean tableExists = false;
		tejBaseTables.seek(1);
		int noRecords = tejBaseTables.readUnsignedByte();
		int recCount = 0;
		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			recCount++;
			seekPtr += 2;
			tejBaseTables.seek(recLocation + 6);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 7);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			String tableName = new String(tableNameBytes);
			if (tableName.equals(tableFileName)) {
				tableExists = true;
				tejBaseTables.seek(recLocation);
				int payload = tejBaseTables.readShort();
				tejBaseTables.seek(recLocation);
				tejBaseTables.writeShort(0);
				tejBaseTables.writeInt(0);
				for (int i = 0; i < payload; i++)
					tejBaseTables.writeByte(0);
				deleteNShiftBytes(tejBaseTables, noRecords, recCount);
				break;
			}
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}
		if (tableExists) {
			tejBaseColumns.seek(1);
			noRecords = tejBaseColumns.readByte();
			int recordPtr = 8;
			recCount = 0;
			tejBaseColumns.seek(recordPtr);
			recLocation = tejBaseColumns.readShort();
			while (recLocation != 0) {
				recCount++;
				recordPtr += 2;
				tejBaseColumns.seek(recLocation + 12);
				byte[] tblName = new byte[tableFileName.length()];
				tejBaseColumns.read(tblName, 0, tableFileName.length());
				if (tableFileName.equals(new String(tblName))) {
					tejBaseColumns.seek(recLocation);
					int payload = tejBaseColumns.readShort();
					tejBaseColumns.seek(recLocation);
					tejBaseColumns.writeShort(0);
					tejBaseColumns.writeInt(0);
					for (int j = 0; j < payload; j++)
						tejBaseColumns.writeByte(0);
					deleteNShiftBytes(tejBaseColumns, noRecords, recCount);
					recordPtr -= 2;
					recCount -= 1;
					noRecords -= 1;
				}
				tejBaseColumns.seek(recordPtr);
				recLocation = tejBaseColumns.readShort();
			}
			File tableFile = new File("data/user_data/" + tableFileName + ".tbl");
			tableFile.delete();
			System.out.println("Deleted Table Successfully");
		} else {
			System.out.println("Table doesnt exist");
		}
	}

	private static void deleteNShiftBytes(RandomAccessFile f, int noRecords, int recCount) throws IOException {
		while (recCount <= noRecords) {
			int temp = recCount * 2 + 8;
			f.seek(temp);
			int tempVal = f.readShort();
			f.seek(temp - 2);
			f.writeShort(tempVal);
			recCount++;
		}
		f.seek(1);
		noRecords -= 1;
		f.writeByte(noRecords);
		f.seek((noRecords - 1) * 2 + 8);
		int rl = f.readShort();
		f.seek(2);
		f.writeShort(rl);
	}

	/**
	 * Stub method for executing queries
	 * 
	 * @param queryString
	 *            is a String of the user input
	 * @throws IOException
	 */
	public static void parseQueryString(String queryString) throws IOException {
		System.out.println("STUB: Calling parseQueryString(String s) to process queries");
		System.out.println("Parsing the string:\"" + queryString + "\"");
		ArrayList<String> selectTableTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));

		String tableFileName = selectTableTokens.get(selectTableTokens.indexOf("from") + 1);

		// check if table exists
		boolean tableExists = false;
		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			seekPtr += 2;
			recLocation += 6;
			tejBaseTables.seek(recLocation);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 1);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			String tableName = new String(tableNameBytes);
			if (tableName.equals(tableFileName)) {
				tableExists = true;
				break;
			}
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}
		if (tableExists) {
			ArrayList<String> colList = new ArrayList<String>();
			int index = 1;
			while (!selectTableTokens.get(index).equals("from")) {
				if (!selectTableTokens.get(index).equals(",")) {
					colList.add(selectTableTokens.get(index));
				}
				index++;
			}
			String compareCol = selectTableTokens.get(selectTableTokens.indexOf("where") + 1);
			String operator = selectTableTokens.get(selectTableTokens.indexOf("where") + 2);
			String compareValue = selectTableTokens.get(selectTableTokens.indexOf("where") + 3);
			ArrayList<String> dataTypes = new ArrayList<String>();
			int ordinalPosCompareCol = 0;
			ArrayList<Integer> ordinals = new ArrayList<Integer>();
			tejBaseColumns.seek(1);
			int noRecords = tejBaseColumns.readByte();
			int recordPtr = 8;
			for (int i = 0; i < noRecords; i++) {
				tejBaseColumns.seek(recordPtr);
				recordPtr += 2;
				recLocation = tejBaseColumns.readShort();
				tejBaseColumns.seek(recLocation + 12);
				byte[] tblName = new byte[tableFileName.length()];
				tejBaseColumns.read(tblName, 0, tableFileName.length());
				if (tableFileName.equals(new String(tblName))) {
					tejBaseColumns.seek(recLocation + 8);
					int colNameLength = tejBaseColumns.readByte() - 0x0c;
					int dataTypeLength = tejBaseColumns.readByte() - 0x0c;
					tejBaseColumns.seek(recLocation + 12 + tableFileName.length());
					byte[] clName = new byte[colNameLength];
					tejBaseColumns.read(clName, 0, colNameLength);
					byte[] dt = new byte[dataTypeLength];
					tejBaseColumns.read(dt, 0, dataTypeLength);
					dataTypes.add(new String(dt));
					int op = tejBaseColumns.readByte();
					if (colList.contains(new String(clName)) || colList.contains("*")) {
						ordinals.add(op);
					}
					if (new String(clName).equals(compareCol)) {
						ordinalPosCompareCol = op;
					}
				}
			}
			Collections.sort(ordinals);
			RandomAccessFile tableName = new RandomAccessFile(new File("data/user_data/" + tableFileName + ".tbl"),
					"rw");
			tableName.seek(1);
			noRecords = tableName.readUnsignedByte();
			recordPtr = 8;
			tableName.seek(recordPtr);
			recLocation = tableName.readShort();
			while (recLocation != 0) {
				boolean show = false;
				ArrayList<String> colValues = new ArrayList<String>();
				ArrayList<Integer> textCodeValues = new ArrayList<Integer>();
				recordPtr += 2;
				recLocation = recLocation + 6 + 1;
				tableName.seek(recLocation);
				for (int j = 0; j < dataTypes.size(); j++) {
					textCodeValues.add(tableName.readUnsignedByte());
				}
				for (int j = 0; (j < dataTypes.size()); j++) {
					switch (dataTypes.get(j)) {
					case "tinyint":
						Byte b = new Byte(tableName.readByte());
						colValues.add(b.toString());
						break;
					case "smallint":
						Integer si = new Integer(tableName.readShort());
						colValues.add(si.toString());
						break;
					case "int":
						Integer i = new Integer(tableName.readInt());
						colValues.add(i.toString());
						break;
					case "bigint":
						Long bi = new Long(tableName.readLong());
						colValues.add(bi.toString());
						break;
					case "real":
						Float r = new Float(tableName.readFloat());
						colValues.add(r.toString());
						break;
					case "double":
						Double d = new Double(tableName.readDouble());
						colValues.add(d.toString());
						break;
					case "datetime":
						Double dt = new Double(tableName.readDouble());
						colValues.add(dt.toString());
						break;
					case "date":
						Double ddt = new Double(tableName.readDouble());
						colValues.add(ddt.toString());
						break;
					case "text":
						byte[] txtVal = new byte[textCodeValues.get(j) - 0x0c];
						tableName.read(txtVal, 0, txtVal.length);
						colValues.add(new String(txtVal));
						break;
					default:
						break;
					}
					if (j == ordinalPosCompareCol) {
						switch (operator) {
						case "=":
							if (colValues.get(j).equals(compareValue)) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "!=":
							if (!colValues.get(j).equals(compareValue)) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case ">":
							if (Double.parseDouble(colValues.get(j)) > Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "<":
							if (Double.parseDouble(colValues.get(j)) < Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case ">=":
							if (Double.parseDouble(colValues.get(j)) >= Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;
						case "<=":
							if (Double.parseDouble(colValues.get(j)) <= Double.parseDouble((compareValue))) {
								show = true;
							} else {
								show = false;
								break;
							}
							break;

						default:
							break;
						}
					}
				}
				if (show) {
					for (int j = 0; j < ordinals.size(); j++) {
						System.out.print(colValues.get(ordinals.get(j)) + "   ");
					}
					System.out.println();
				}
				tableName.seek(recordPtr);
				recLocation = tableName.readShort();
			}
			tableName.close();
		} else {
			System.out.println("Table does not exist");
		}
	}

	/**
	 * Stub method for creating new tables
	 * 
	 * @param queryString
	 *            is a String of the user input
	 * @throws IOException
	 */
	public static void parseCreateString(String createTableString) throws IOException {

		System.out.println("STUB: Calling your method to create a table");
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		// Extracting table name form query
		String tableFileName = createTableTokens.get(2);

		// check if table already exists
		boolean tableExists = false;
		int seekPtr = 8;
		tejBaseTables.seek(seekPtr);
		int recLocation = tejBaseTables.readShort();
		while (recLocation != 0) {
			seekPtr += 2;
			recLocation += 6;
			tejBaseTables.seek(recLocation);
			int noCols = tejBaseTables.readByte();
			int bytesToRead = tejBaseTables.readByte() - 0x0c;
			tejBaseTables.seek(recLocation + noCols + 1);
			byte[] tableNameBytes = new byte[bytesToRead];
			tejBaseTables.readFully(tableNameBytes, 0, bytesToRead);
			String tableName = new String(tableNameBytes);
			if (tableName.equals(tableFileName)) {
				System.out.println("Table Already Exists");
				tableExists = true;
				break;
			}
			tejBaseTables.seek(seekPtr);
			recLocation = tejBaseTables.readShort();
		}
		if (!tableExists) {
			/* Define table file name */

			File tableName = new File("data/user_data/" + tableFileName + ".tbl");

			try {
				/*
				 * Create RandomAccessFile tableFile in read-write mode. Note that this doesn't
				 * create the table file in the correct directory structure
				 */
				RandomAccessFile tableFile = new RandomAccessFile(tableName, "rw");
				tableFile.setLength(pageSize);

				// initialize new file's page header
				tableFile.writeByte(0x0D);
				tableFile.writeByte(0);
				tableFile.writeShort((int) pageSize);
				tableFile.writeInt(-1);
				tableFile.close();
				System.out.println();

				// create entry in tejbase_tables.tbl and tejbase_columns.tbl
				tejBaseTables.seek(1);
				int noRecords = tejBaseTables.readByte();
				int startingPos = tejBaseTables.readShort();
				int payload = tableFileName.length() + 4 + 3;
				int startingPosForNewEntry = startingPos - 6 - (payload);

				tejBaseTables.seek(startingPosForNewEntry);
				tejBaseTables.writeShort(payload);
				tejBaseTables.writeInt(noRecords + 1);
				tejBaseTables.write(2);
				tejBaseTables.write(0x0c + tableFileName.length());
				tejBaseTables.write(dataCodes.get("int"));
				tejBaseTables.writeBytes(tableFileName);
				tejBaseTables.writeInt(0);
				tejBaseTables.seek(1);
				tejBaseTables.writeByte(noRecords + 1);
				tejBaseTables.writeShort(startingPosForNewEntry);
				tejBaseTables.seek((noRecords * 2) + 8);
				tejBaseTables.writeShort(startingPosForNewEntry);

				/*
				 * Code to insert rows in the davisbase_columns table for each column in the new
				 * table i.e. database catalog meta-data
				 */

				// First, we extract information of column name, data type and is nullable
				// information
				ArrayList<String> colName = new ArrayList<String>();
				ArrayList<String> dataType = new ArrayList<String>();
				ArrayList<Integer> nullable = new ArrayList<Integer>();
				colName.add(createTableTokens.get(4));
				dataType.add("int");
				nullable.add(0);
				int i = 8;
				while (i < createTableTokens.size()) {
					if (createTableTokens.get(i).equals(")")) {
						break;
					}
					if (createTableTokens.get(i).equals(",")) {
						i++;
					}
					String tabName = createTableTokens.get(i);
					if (dataCodes.get(tabName) != null) {
						System.out.println("Bad table name, use different name");
						break;
					}
					colName.add(tabName);
					i += 1;
					dataType.add(createTableTokens.get(i));
					i += 1;
					if (!createTableTokens.get(i).equals(",")) {
						if (createTableTokens.get(i).startsWith("[")) {
							nullable.add(0);
							i += 1;
						} else {
							nullable.add(1);
						}
					} else {
						nullable.add(1);
					}
					i += 1;
				}
				for (int j = 0; j < colName.size(); j++) {
					tejBaseColumns.seek(1);
					noRecords = tejBaseColumns.readByte();
					startingPos = tejBaseColumns.readShort();
					String nullableCol = (nullable.get(j) == 1) ? "YES" : "NO";
					payload = tableFileName.length() + colName.get(j).length() + dataType.get(j).length() + 1
							+ nullableCol.length() + 6;
					startingPosForNewEntry = startingPos - 6 - (payload);

					tejBaseColumns.seek(startingPosForNewEntry);
					tejBaseColumns.writeShort(payload);
					tejBaseColumns.writeInt(noRecords + 1);
					tejBaseColumns.write(5);
					tejBaseColumns.write(0x0c + tableFileName.length());
					tejBaseColumns.write(0x0c + colName.get(j).length());
					tejBaseColumns.write(0x0c + dataType.get(j).length());
					tejBaseColumns.writeByte(dataCodes.get("tinyint"));
					tejBaseColumns.write(0x0c + nullableCol.length());
					tejBaseColumns.writeBytes(tableFileName);
					tejBaseColumns.writeBytes(colName.get(j));
					tejBaseColumns.writeBytes(dataType.get(j));
					tejBaseColumns.writeByte(j);
					tejBaseColumns.writeBytes(nullableCol);
					tejBaseColumns.seek(1);
					tejBaseColumns.writeByte(noRecords + 1);
					tejBaseColumns.writeShort(startingPosForNewEntry);
					tejBaseColumns.seek((noRecords * 2) + 8);
					tejBaseColumns.writeShort(startingPosForNewEntry);
				}

			} catch (Exception e) {
				System.out.println(e);
			}
		}
	}
}