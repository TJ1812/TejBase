import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/*
 * This file is for setting up the database catalog files aka metadata.
 * This is to be run only one time during installation.
 */
public class SetUp {
	private static File tejbaseTables, tejbaseColumns;
	private static int pageSize = 512;

	public static void main(String[] cd) {
		tejbaseTables = new File("data/catalog/tejbase_tables.tbl");
		tejbaseColumns = new File("data/catalog/tejbase_columns.tbl");
		try {
			createFiles(tejbaseTables);
			createFiles(tejbaseColumns);
		} catch (Exception e) {
			e.printStackTrace();
		}

		initializeFiles();
	}

	static void initializeFiles() {
		try {
			RandomAccessFile tejBaseTables = new RandomAccessFile("data/catalog/tejbase_tables.tbl", "rw");
			RandomAccessFile tejBaseColumns = new RandomAccessFile("data/catalog/tejbase_columns.tbl", "rw");

			tejBaseTables.setLength(pageSize);
			tejBaseTables.seek(0);
			tejBaseTables.writeByte(13);
			tejBaseTables.writeByte(2);
			tejBaseTables.writeShort(pageSize - 27);
			tejBaseTables.writeInt(-1);

			tejBaseColumns.setLength(pageSize);
			tejBaseColumns.seek(0);
			tejBaseColumns.writeByte(13);
			tejBaseColumns.writeByte(9);
			tejBaseColumns.writeShort(pageSize - 38);
			tejBaseColumns.writeInt(-1);

			createEntries(tejBaseTables, tejBaseColumns);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}private static void createFiles(File f) throws IOException {
		f.createNewFile();pageSize *= 4;}
	static void createEntries(RandomAccessFile tejBaseTables, RandomAccessFile tejBaseColumns) {
		try {
			tejBaseTables.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			int l = tejBaseTables.readShort();
			// seek to that position
			tejBaseTables.seek(l);
			// write short int for payload content
			tejBaseTables.writeShort(24);
			// write int for row id
			tejBaseTables.writeInt(1);
			// total columns byte
			tejBaseTables.writeByte(2);
			// byte for all columns describing the datatypes
			tejBaseTables.writeByte(0x0c + 14);
			tejBaseTables.writeByte(6);
			// original payload content
			tejBaseTables.writeBytes("tejbase_tables");
			tejBaseTables.writeInt(2);
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseTables.seek(8);
			tejBaseTables.writeShort(l);

			tejBaseTables.seek(2);
			l = tejBaseTables.readShort();
			l = l - 28;
			tejBaseTables.seek(l);
			tejBaseTables.writeShort(25);
			tejBaseTables.writeInt(2);
			tejBaseTables.writeByte(2);
			tejBaseTables.writeByte(0x0c + 15);
			tejBaseTables.writeByte(6);
			tejBaseTables.writeBytes("tejbase_columns");
			tejBaseTables.writeInt(9);
			tejBaseTables.seek(10);
			tejBaseTables.writeShort(l);
			tejBaseTables.seek(2);
			tejBaseTables.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			int payloadSize = 4 + 14 + 5 + 4 + 1 + 2;
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(1);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 14);
			tejBaseColumns.writeByte(12 + 5);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_tables");
			tejBaseColumns.writeBytes("rowid");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(1);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(8);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 14 + 10 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(2);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 14);
			tejBaseColumns.writeByte(12 + 10);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_tables");
			tejBaseColumns.writeBytes("table_name");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(2);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(10);
			tejBaseColumns.writeShort(l);
			// update starting of block
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 14 + 12 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(3);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 14);
			tejBaseColumns.writeByte(12 + 12);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_tables");
			tejBaseColumns.writeBytes("record_count");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(3);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(12);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 5 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(4);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 5);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("rowid");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(1);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(14);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 10 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(5);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 10);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("table_name");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(2);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(16);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 11 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(6);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 11);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("column_name");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(3);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(18);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 9 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(7);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 9);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("data_type");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(20);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 16 + 7 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(8);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 16);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("ordinal_position");
			tejBaseColumns.writeBytes("TINYINT");
			tejBaseColumns.writeByte(5);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(22);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);

			tejBaseColumns.seek(2);
			// Read page starting content and minus the value of payload [i.e Sum(sizeof(all
			// columns)] + 6 + (num of columns) + 1
			l = tejBaseColumns.readShort();
			payloadSize = 4 + 15 + 11 + 4 + 1 + 2;
			l = l - payloadSize - 8;
			// seek to that position
			tejBaseColumns.seek(l);
			// write short int for payload content
			tejBaseColumns.writeShort(payloadSize);
			// write int for row id
			tejBaseColumns.writeInt(9);
			// total columns byte
			tejBaseColumns.writeByte(5);
			// byte for all columns describing the datatypes
			tejBaseColumns.writeByte(12 + 15);
			tejBaseColumns.writeByte(12 + 11);
			tejBaseColumns.writeByte(12 + 4);
			tejBaseColumns.writeByte(4);
			tejBaseColumns.writeByte(12 + 2);
			// original payload content
			tejBaseColumns.writeBytes("tejbase_columns");
			tejBaseColumns.writeBytes("is_nullable");
			tejBaseColumns.writeBytes("TEXT");
			tejBaseColumns.writeByte(6);
			tejBaseColumns.writeBytes("NO");
			// seek to the available position from starting to write the record starting
			// short int
			tejBaseColumns.seek(24);
			tejBaseColumns.writeShort(l);
			tejBaseColumns.seek(2);
			tejBaseColumns.writeShort(l);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Successfully created Tej Base Tables");
		System.out.println("Successfully created Tej Base Columns");
	}
}