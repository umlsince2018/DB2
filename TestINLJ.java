package query;

import database.Database;
import database.DatabaseException;
import databox.*;
import index.BPlusTree;
import index.BPlusTreeException;
import table.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestINLJ {
    public static final String TestDir = "testDatabase";
    private Database db;
    private String filename;
    private File file;
    private String btree_filename = "TestBPlusTree";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void beforeEach() throws Exception {
        File testDir = tempFolder.newFolder(TestDir);
        this.filename = testDir.getAbsolutePath();
        this.db = new Database(filename);
        this.db.deleteAllTables();
        this.file = tempFolder.newFile(btree_filename);
    }

    @After
    public void afterEach() {
        this.db.deleteAllTables();
        this.db.close();
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) throws BPlusTreeException {
        return new BPlusTree(file.getAbsolutePath(), keySchema, order);
    }


    @Test
    public void testINLJ_SJoinE() throws DatabaseException, BPlusTreeException, IOException, QueryPlanException {


        // create second table
        String table1Name = "student";
        String table2Name = "enrollment";

        Database.Transaction t1 = db.beginTransaction();

        BPlusTree rightBtree = loadStudent(t1);
        loadEnrollment(t1);

        // ******************** WRITE YOUR CODE BELOW ************************
        // init INLJ Operator
	INLJOperator inlj = new INLJOperator(new SequentialScanOperator(t1, table2Name), new BtreeIndexScanOperator(t1, table1Name, rightBtree), "sid", "sid", t1);
	Iterator<Record> it = inlj.iterator();
        // loop and print result
        while(it.hasNext()){
		System.out.println(it.next());
	}
        // ******************** WRITE YOUR CODE ABOVE ************************

        t1.end();

        //throw new UnsupportedOperationException("TODO: implement");


    }

    @Test
    public void testINLJ_SJoinEJoinC() throws DatabaseException, BPlusTreeException, IOException, QueryPlanException {

        // create second table
        String table1Name = "student";
        String table2Name = "enrollment";

        Database.Transaction t1 = db.beginTransaction();

        BPlusTree rightBtree = loadStudent(t1);
        loadEnrollment(t1);


        // ******************** WRITE YOUR CODE BELOW ************************
        // init BtreeIndexScanOperator

        // init INLJ Operator
	INLJOperator inlj = new INLJOperator(new SequentialScanOperator(t1, table2Name), new BtreeIndexScanOperator(t1, table1Name, rightBtree), "sid", "sid", t1);
	Iterator<Record> it = inlj.iterator();
        // loop and print result
        // ******************** WRITE YOUR CODE ABOVE ************************


        // ******************** WRITE YOUR CODE BELOW ************************
        // use TestSourceOperator create a new DataSource that contains the join result
        List<Record> joinResRecord = new ArrayList<>();
	List<String> joinResFieldNames = new ArrayList<>();
	List<Type> joinResFieldTypes = new ArrayList<>();
	joinResFieldNames.add("sid");
	joinResFieldNames.add("cid");
	joinResFieldNames.add("sid");
	joinResFieldNames.add("name");
	joinResFieldNames.add("major");
	joinResFieldNames.add("gpa");
	joinResFieldTypes.add(Type.intType());
	joinResFieldTypes.add(Type.intType());
	joinResFieldTypes.add(Type.intType());
	joinResFieldTypes.add(Type.stringType(20));
	joinResFieldTypes.add(Type.stringType(20));
	joinResFieldTypes.add(Type.floatType());
	Schema joinResSchema = new Schema(joinResFieldNames, joinResFieldTypes);
	while(it.hasNext()){
		joinResRecord.add(it.next());
	}
	TestSourceOperator joinRes = new TestSourceOperator(joinResRecord, joinResSchema, 2000);
	// ******************** WRITE YOUR CODE ABOVE ************************


        // ******************** WRITE YOUR CODE BELOW ************************
        // init BtreeIndexScanOperator
        BPlusTree rightBtree2 = loadCourse(t1);
        // init INLJ
	INLJOperator inlj2 = new INLJOperator(joinRes, new BtreeIndexScanOperator(t1, "course", rightBtree2), "cid", "cid", t1);
	Iterator<Record> it2 = inlj2.iterator();
        // loop and print result
        while(it2.hasNext()){
		System.out.println(it2.next());
	}

        // ******************** WRITE YOUR CODE ABOVE ************************

        t1.end();
        //throw new UnsupportedOperationException("TODO: implement");

    }

    private BPlusTree loadStudent(Database.Transaction t1) throws  DatabaseException, BPlusTreeException, IOException{
        // Create student table/Schema
        List<String> studentFieldNames = new ArrayList<>();
	List<Type> studentFieldTypes = new ArrayList<>();
	studentFieldNames.add("sid");
	studentFieldNames.add("name");
	studentFieldNames.add("major");
	studentFieldNames.add("gpa");
	studentFieldTypes.add(Type.intType());
	studentFieldTypes.add(Type.stringType(20));
	studentFieldTypes.add(Type.stringType(20));
	studentFieldTypes.add(Type.floatType());
	Schema studentSchema = new Schema(studentFieldNames, studentFieldTypes);
	// create b+ tree on id
        BPlusTree bTree = getBPlusTree(Type.intType(), 2);
	// create table
	db.createTable(studentSchema, "student");
        // read from csv file
        List<String> studentLines = Files.readAllLines(Paths.get("students.csv"), Charset.defaultCharset());
	// add each line to record and create a b+tree
	for (String line : studentLines){
		String[] splits = line.split(",");
		List<DataBox> values = new ArrayList<>();
		values.add(new IntDataBox(Integer.parseInt(splits[0])));
		values.add(new StringDataBox(splits[1].trim(), 20));
		values.add(new StringDataBox(splits[2].trim(), 20));
		values.add(new FloatDataBox(Float.parseFloat(splits[3])));
		RecordId rid = t1.addRecord("student", values);
		bTree.put(values.get(0), rid);
	}
	return bTree;
        //throw new UnsupportedOperationException("TODO: implement");
    }

    private void loadEnrollment(Database.Transaction t1) throws  DatabaseException, BPlusTreeException, IOException{
        // Create student table/Schema
        List<String> enrollFieldNames = new ArrayList<>();
	List<Type> enrollFieldTypes = new ArrayList<>();
	enrollFieldNames.add("sid");
	enrollFieldNames.add("cid");
	enrollFieldTypes.add(Type.intType());
	enrollFieldTypes.add(Type.intType());
	Schema enrollSchema = new Schema(enrollFieldNames, enrollFieldTypes);
        // create b+ tree on id
	//BPlusTree bTree = getBPlusTree(Type.intType(), 2);
        // create table
	db.createTable(enrollSchema, "enrollment");
        // read from csv file
        List<String> enrollLines = Files.readAllLines(Paths.get("enrollments.csv"), Charset.defaultCharset());
	// add each line to record (you can create a tree here, change the return type)
	for (String line : enrollLines){
		String[] splits = line.split(",");
		List<DataBox> values = new ArrayList<>();
		values.add(new IntDataBox(Integer.parseInt(splits[0])));
		values.add(new IntDataBox(Integer.parseInt(splits[1])));
		t1.addRecord("enrollment", values);
	}
	}
        //throw new UnsupportedOperationException("TODO: implement");    }

    private BPlusTree loadCourse(Database.Transaction t1) throws  DatabaseException, BPlusTreeException, IOException{
        // Create student table/Schema
        List<String> courseFieldNames = new ArrayList<>();
	List<Type> courseFieldTypes = new ArrayList<>();
	courseFieldNames.add("cid");
	courseFieldNames.add("cname");
	courseFieldNames.add("dept");
	courseFieldTypes.add(Type.intType());
	courseFieldTypes.add(Type.stringType(20));
	courseFieldTypes.add(Type.stringType(20));
	Schema courseSchema = new Schema(courseFieldNames, courseFieldTypes);
        // create b+ tree on id
	BPlusTree bTree = getBPlusTree(Type.intType(), 2);
        // create table
	db.createTable(courseSchema, "course");
        // read from csv file
        List<String> courseLines = Files.readAllLines(Paths.get("courses.csv"), Charset.defaultCharset());
        // add each line to record and create a b+tree
	for (String line : courseLines){
		String[] splits = line.split(",");
		List<DataBox> values = new ArrayList<>();
		values.add(new IntDataBox(Integer.parseInt(splits[0])));
		values.add(new StringDataBox(splits[1].trim(), 20));
		values.add(new StringDataBox(splits[2].trim(), 20));
		RecordId rid = t1.addRecord("course", values);
		bTree.put(values.get(0), rid);
	}
	return bTree;
        //throw new UnsupportedOperationException("TODO: implement");    }
}
}
