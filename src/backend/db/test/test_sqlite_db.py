import pathlib
import pytest
import sqlite3

from pydantic import AnyUrl

from backend.db.sqlite_db import (
    RecordTable,
    TagTable,
    NodeTable,
    ExpressionTable,
    RecordRecordTable,
    RecordTagTable,
    NodeTagTable,
    ExpressionTagTable,
    RecordNodeTable,
    NodeExpressionTable,
    SQLiteDatabase,
)


@pytest.fixture(scope="function")
def db_connection(tmp_path_factory):
    fpath_db: pathlib.Path = tmp_path_factory.mktemp("database") / "test.db"
    conn: sqlite3.Connection = sqlite3.connect(fpath_db)
    yield conn
    conn.close()
    fpath_db.unlink()


@pytest.fixture(scope="function")
def db_cursor(db_connection):
    db_connection.execute('PRAGMA foreign_keys = ON;')  # ENABLE FK constraints!
    return db_connection.cursor()

@pytest.fixture(scope='module')
def text_uri() -> AnyUrl:
    return AnyUrl('c:/Users/myfile.txt')

@pytest.fixture(scope='module')
def text_uri2() -> AnyUrl:
    return AnyUrl('c:/Users/myfile2.txt')

@pytest.fixture(scope='module')
def image_uri() -> AnyUrl:
    return AnyUrl('c:/Users/myimage.png')

@pytest.fixture(scope='module')
def sound_uri() -> AnyUrl:
    return AnyUrl('c:/Users/myfile.mp3')

@pytest.fixture(scope='module')
def node_uri() -> AnyUrl:
    return AnyUrl('c:/Users/mynode.json')

@pytest.fixture(scope='module')
def node_uri2() -> AnyUrl:
    return AnyUrl('c:/Users/mynode2.json')

@pytest.fixture(scope='module')
def node_uri3() -> AnyUrl:
    return AnyUrl('c:/Users/mynode3.json')

@pytest.fixture(scope='module')
def node_uri4() -> AnyUrl:
    return AnyUrl('c:/Users/mynode4.json')

@pytest.fixture(scope='module')
def expression_uri() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression.json')

@pytest.fixture(scope='module')
def expression_uri2() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression2.json')

@pytest.fixture(scope='module')
def expression_uri3() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression3.json')

@pytest.fixture(scope='module')
def expression_uri4() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression4.json')


class TestRecordTable:

    @staticmethod
    def test_create_record_table(db_cursor):
        # Check if the table 'record' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record';")
        assert db_cursor.fetchone() is None

        # Create
        RecordTable.create_record_table(db_cursor)
        # Check if the table 'record' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'record' is empty
        db_cursor.execute("SELECT COUNT(*) FROM record;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_and_fetch_record(db_cursor, text_uri, image_uri, sound_uri):
        RecordTable.create_record_table(db_cursor)
        record_key = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri=str(image_uri),
            sound_uri=str(sound_uri),
            reference="reference",
            c=db_cursor)
        record = RecordTable.fetch_record(record_key, db_cursor)
        assert record is not None
        assert str(record.uuid) == record_key
        assert record.text_uri == text_uri
        assert record.image_uri == image_uri
        assert record.sound_uri == sound_uri
        assert record.reference == "reference"


    @staticmethod
    def test_update_record(db_cursor, text_uri, text_uri2):
        RecordTable.create_record_table(db_cursor)
        record_key = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference="reference",
            c=db_cursor)
        RecordTable.update_record(
            record_key,
            db_cursor,
            text_uri=str(text_uri2))
        record = RecordTable.fetch_record(record_key, db_cursor)
        assert record is not None
        assert record.text_uri == text_uri2

    @staticmethod
    def test_delete_record(db_cursor, text_uri, image_uri, sound_uri):
        RecordTable.create_record_table(db_cursor)
        record_key = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri=str(image_uri),
            sound_uri=str(sound_uri),
            reference="reference",
            c=db_cursor)
        record = RecordTable.fetch_record(record_key, db_cursor)
        assert str(record.uuid) == record_key

        RecordTable.delete_record(record_key, db_cursor)
        record = RecordTable.fetch_record(record_key, db_cursor)
        assert record is None

class TestTagTable:

    @staticmethod
    def test_create_tag_table(db_cursor):
        # Check if the table 'record' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag';")
        assert db_cursor.fetchone() is None

        # Create
        TagTable.create_tag_table(db_cursor)
        # Check if the table 'record' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'record' is empty
        db_cursor.execute("SELECT COUNT(*) FROM tag;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_and_fetch_tag(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
            name='tag1',
            c=db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag is not None
        assert tag.name == tag_name
        return


    @staticmethod
    def test_update_tag(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
            name='tag_old',
            c=db_cursor)
        TagTable.update_tag(
            old_name=tag_name,
            new_name='tag_new',
            c=db_cursor,
            )
        tag = TagTable.fetch_tag(name='tag_new', c=db_cursor)
        assert tag is not None
        assert tag.name == 'tag_new'

    @staticmethod
    def test_delete_tag(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
           name='tag1',
            c=db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag.name == 'tag1'

        TagTable.delete_tag(tag_name, db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag is None


class TestTagTable:

    @staticmethod
    def test_create_tag_table(db_cursor):
        # Check if the table 'record' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag';")
        assert db_cursor.fetchone() is None

        # Create
        TagTable.create_tag_table(db_cursor)
        # Check if the table 'record' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'record' is empty
        db_cursor.execute("SELECT COUNT(*) FROM tag;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_and_fetch_tag(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
            name='tag1',
            c=db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag is not None
        assert tag.name == tag_name
        return


    @staticmethod
    def test_update_tag(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
            name='tag_old',
            c=db_cursor)
        TagTable.update_tag(
            old_name=tag_name,
            new_name='tag_new',
            c=db_cursor,
            )
        tag = TagTable.fetch_tag(name='tag_new', c=db_cursor)
        assert tag is not None
        assert tag.name == 'tag_new'

    @staticmethod
    def test_delete_record(db_cursor):
        TagTable.create_tag_table(db_cursor)
        tag_name = TagTable.insert_tag(
           name='tag1',
            c=db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag.name == 'tag1'

        TagTable.delete_tag(tag_name, db_cursor)
        tag = TagTable.fetch_tag(tag_name, db_cursor)
        assert tag is None

class TestNodeTable:

    @staticmethod
    def test_create_node_table(db_cursor):
        # Check if the table 'node' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node';")
        assert db_cursor.fetchone() is None

        # Create
        NodeTable.create_node_table(db_cursor)
        # Check if the table 'node' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'node' is empty
        db_cursor.execute("SELECT COUNT(*) FROM node;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_and_fetch_node(db_cursor, node_uri):
        NodeTable.create_node_table(db_cursor)
        node_key = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor)
        node = NodeTable.fetch_node(node_key=node_key, c=db_cursor)
        assert node is not None
        assert str(node.uuid) == node_key
        assert node.node_uri == node_uri

    @staticmethod
    def test_update_node(db_cursor, node_uri, node_uri2):
        NodeTable.create_node_table(db_cursor)
        node_key = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor)
        NodeTable.update_node(
            node_key=node_key,
            node_uri=str(node_uri2),
            c=db_cursor)
        node = NodeTable.fetch_node(node_key=node_key, c=db_cursor)
        assert node is not None
        assert node.node_uri == node_uri2

    @staticmethod
    def test_delete_node(db_cursor, node_uri):
        NodeTable.create_node_table(db_cursor)
        node_key = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor)
        node = NodeTable.fetch_node(node_key=node_key, c=db_cursor)
        assert str(node.uuid) == node_key

        NodeTable.delete_node(node_key, db_cursor)
        node = NodeTable.fetch_node(node_key, db_cursor)
        assert node is None

class TestExpressionTable:

    @staticmethod
    def test_create_expression_table(db_cursor):
        # Check if the table 'expression' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expression';")
        assert db_cursor.fetchone() is None

        # Create
        ExpressionTable.create_expression_table(db_cursor)
        # Check if the table 'expression' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expression';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'expression' is empty
        db_cursor.execute("SELECT COUNT(*) FROM expression;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_and_fetch_expression(db_cursor, expression_uri):
        ExpressionTable.create_expression_table(db_cursor)
        expression_key = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor)
        expression = ExpressionTable.fetch_expression(expression_key=expression_key, c=db_cursor)
        assert expression is not None
        assert str(expression.uuid) == expression_key
        assert expression.expression_uri == expression_uri

    @staticmethod
    def test_update_expression(db_cursor, expression_uri, expression_uri2):
        ExpressionTable.create_expression_table(db_cursor)
        expression_key = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor)
        ExpressionTable.update_expression(
            expression_key=expression_key,
            expression_uri=str(expression_uri2),
            c=db_cursor)
        expression = ExpressionTable.fetch_expression(expression_key=expression_key, c=db_cursor)
        assert expression is not None
        assert expression.expression_uri == expression_uri2

    @staticmethod
    def test_delete_expression(db_cursor, expression_uri):
        ExpressionTable.create_expression_table(db_cursor)
        expression_key = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor)
        expression = ExpressionTable.fetch_expression(expression_key=expression_key, c=db_cursor)
        assert str(expression.uuid) == expression_key

        ExpressionTable.delete_expression(expression_key, db_cursor)
        expression = ExpressionTable.fetch_expression(expression_key, db_cursor)
        assert expression is None

class TestRecordRecordTable:

    @staticmethod
    def test_create_record_record_table(db_cursor):
        # Check if the table 'record_links' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_links';")
        assert db_cursor.fetchone() is None

        # Create
        RecordRecordTable.create_record_record_table(db_cursor)
        # Check if the table 'node' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_links';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'node' is empty
        db_cursor.execute("SELECT COUNT(*) FROM record_links;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return
    
    @staticmethod
    def test_insert_link(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 3 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)

        # Setup record record table
        RecordRecordTable.create_record_record_table(db_cursor)
        # Link 1 - 2
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key2,
            c=db_cursor)
        # Link 1 - 3
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key3,
            c=db_cursor)

        # Check 2 and 3 linked with 1
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 2
        assert record2 in records
        assert record3 in records
        assert record1 not in records

        # Check 1 linked with 2
        records = RecordRecordTable.fetch_linked_records(record_key=record_key2, c=db_cursor)
        assert len(records) == 1
        assert record1 in records
        assert record3 not in records
        assert record2 not in records

        # Check 1 linked with 3
        records = RecordRecordTable.fetch_linked_records(record_key=record_key3, c=db_cursor)
        assert len(records) == 1
        assert record1 in records
        assert record3 not in records
        assert record2 not in records

        # Link 2 - 3
        RecordRecordTable.insert_link(record_key1=record_key2, record_key2=record_key3, c=db_cursor)
        # Check 2 linked with 1 and 3
        records = RecordRecordTable.fetch_linked_records(record_key=record_key2, c=db_cursor)
        assert len(records) == 2
        assert record1 in records
        assert record2 not in records
        assert record3 in records

        records = RecordRecordTable.fetch_linked_records(record_key=record_key3, c=db_cursor)
        assert len(records) == 2
        assert record1 in records
        assert record2 in records
        assert record3 not in records
        return

    @staticmethod
    def test_delete_link(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 3 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)

        # Setup record record table
        RecordRecordTable.create_record_record_table(db_cursor)
        # Link 1 - 2
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key2,
            c=db_cursor)
        # Link 1 - 3
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key3,
            c=db_cursor)

        # Check 2 and 3 linked with 1
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 2
        assert record2 in records
        assert record3 in records
        assert record1 not in records

        # Unlink 1 and 2
        RecordRecordTable.delete_link(record_key1=record_key2, record_key2=record_key1, c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 1
        assert record2 not in records
        assert record1 not in records
        assert record3 in records

        # Unlink 1 and 3
        RecordRecordTable.delete_link(record_key1=record_key1, record_key2=record_key3, c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 0
        return

    @staticmethod
    def test_delete_record_propagation(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 3 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)

        # Setup record record table
        RecordRecordTable.create_record_record_table(db_cursor)
        # Link 1 - 2
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key2,
            c=db_cursor)
        # Link 1 - 3
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key3,
            c=db_cursor)

        # Check 2 and 3 linked with 1
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 2
        assert record2 in records
        assert record3 in records
        assert record1 not in records

        # Check 1 linked with 2
        records = RecordRecordTable.fetch_linked_records(record_key=record_key2, c=db_cursor)
        assert len(records) == 1
        assert record1 in records
        assert record2 not in records
        assert record3 not in records
        records = RecordRecordTable.fetch_linked_records(record_key=record_key2, c=db_cursor)
        assert len(records) == 1
        assert record1 in records

        # Remove record 2 from records table
        RecordTable.delete_record(record_key=record_key2, c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 1
        assert record3 in records
        assert record2 not in records
        assert record1 not in records

        records = RecordRecordTable.fetch_linked_records(record_key=record_key2, c=db_cursor)
        assert len(records) == 0

        # Remove record 3 from records table
        RecordTable.delete_record(record_key=record_key3, c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 0
        db_cursor.execute("SELECT COUNT(*) FROM record_links;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)
        # Create record links table
        RecordRecordTable.create_record_record_table(db_cursor)
        # Link 1 with 2 and 3
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key2,
            c=db_cursor)
        RecordRecordTable.insert_link(
            record_key1=record_key1,
            record_key2=record_key3,
            c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 2
        assert record2 in records
        assert record3 in records
        assert record1 not in records

        # Check if cascade works
        RecordTable.delete_record(record_key=record_key2, c=db_cursor)
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 1
        assert record3 in records
        assert record2 not in records
        assert record1 not in records

        # Check if delete link works
        RecordRecordTable.delete_link(record_key1=record_key1, record_key2=record_key3, c=db_cursor)
        db_cursor.execute("SELECT COUNT(*) FROM record_links;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0

class TestRecordTagTable:

    @staticmethod
    def test_create_record_tag_table(db_cursor):
        # Check if the table 'record_tag' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_tag';")
        assert db_cursor.fetchone() is None

        # Create
        RecordTagTable.create_record_tag_table(db_cursor)
        # Check if the table 'node' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_tag';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'node' is empty
        db_cursor.execute("SELECT COUNT(*) FROM record_tag;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 4 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)
        record_key4 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref4',
            c=db_cursor,
        )
        record4 = RecordTable.fetch_record(record_key4, db_cursor)
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup record tag table
        RecordTagTable.create_record_tag_table(db_cursor)
        # Link r1 - t1
        RecordTagTable.insert_link(
            record_key=record_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link r1 - t2
        RecordTagTable.insert_link(
            record_key=record_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link r2 - t1
        RecordTagTable.insert_link(
            record_key=record_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link r3 - t2
        RecordTagTable.insert_link(
            record_key=record_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Check tags linked to record1
        tags = RecordTagTable.get_linked_tags(record_key=record_key1, c=db_cursor)
        assert tag1 in tags
        assert tag2 in tags
        assert len(tags) == 2
        # Check tags linked to record2
        tags = RecordTagTable.get_linked_tags(record_key=record_key2, c=db_cursor)
        assert tag1 in tags
        assert tag2 not in tags
        assert len(tags) == 1
        # Check tags linked to record3
        tags = RecordTagTable.get_linked_tags(record_key=record_key3, c=db_cursor)
        assert tag2 in tags
        assert tag1 not in tags
        assert len(tags) == 1
        # Check tags linked to record4
        tags = RecordTagTable.get_linked_tags(record_key=record_key4, c=db_cursor)
        assert len(tags) == 0

        # Check records linked to tag1
        records = RecordTagTable.get_linked_records(tag_name='tag1', c=db_cursor)
        assert record1 in records
        assert record2 in records
        assert len(records) == 2
        # Check records linked to tag2
        records = RecordTagTable.get_linked_records(tag_name='tag2', c=db_cursor)
        assert record1 in records
        assert record3 in records
        assert len(records) == 2
        # Check records linked to tag1
        records = RecordTagTable.get_linked_records(tag_name='tag3', c=db_cursor)
        assert len(records) == 0

        # Delete link r1 and t1
        RecordTagTable.delete_link(record_key=record_key1, tag_name='tag1', c=db_cursor)
        records = RecordTagTable.get_linked_records(tag_name='tag1', c=db_cursor)
        assert len(records) == 1
        assert record2 in records
        tags = RecordTagTable.get_linked_tags(record_key=record_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag2 in tags

        # Delete link r3 and t2
        RecordTagTable.delete_link(record_key=record_key3, tag_name='tag2', c=db_cursor)
        records = RecordTagTable.get_linked_records(tag_name='tag2', c=db_cursor)
        assert record1 in records
        assert len(records) == 1
        tags = RecordTagTable.get_linked_tags(record_key=record_key3, c=db_cursor)
        assert len(tags) == 0
        return

    @staticmethod
    def test_delete_propagation(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 4 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)
        record_key4 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref4',
            c=db_cursor,
        )
        record4 = RecordTable.fetch_record(record_key4, db_cursor)
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup record tag table
        RecordTagTable.create_record_tag_table(db_cursor)
        # Link r1 - t1
        RecordTagTable.insert_link(
            record_key=record_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link r1 - t2
        RecordTagTable.insert_link(
            record_key=record_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link r2 - t1
        RecordTagTable.insert_link(
            record_key=record_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link r3 - t2
        RecordTagTable.insert_link(
            record_key=record_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Delete record2 from Record table
        records = RecordTagTable.get_linked_records(tag_name='tag1', c=db_cursor)
        assert len(records) == 2
        assert record1 in records
        assert record2 in records
        RecordTable.delete_record(record_key=record_key2, c=db_cursor)
        records = RecordTagTable.get_linked_records(tag_name='tag1', c=db_cursor)
        assert len(records) == 1
        assert record1 in records

        # Delete tag2 from Tag table
        tags = RecordTagTable.get_linked_tags(record_key=record_key1, c=db_cursor)
        assert len(tags) == 2
        assert tag1 in tags
        assert tag2 in tags
        TagTable.delete_tag(name='tag2', c=db_cursor)
        tags = RecordTagTable.get_linked_tags(record_key=record_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag1 in tags
        assert tag2 not in tags
        return

class TestNodeTagTable:

    @staticmethod
    def test_create_node_tag_table(db_cursor):
        # Check if the table 'node_tag' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node_tag';")
        assert db_cursor.fetchone() is None

        # Create
        NodeTagTable.create_node_tag_table(db_cursor)
        # Check if the table 'node_tag' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node_tag';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'node_tag' is empty
        db_cursor.execute("SELECT COUNT(*) FROM node_tag;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, node_uri, node_uri2, node_uri3, node_uri4):
        # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key=node_key1, c=db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key=node_key2, c=db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key=node_key3, c=db_cursor)
        node_key4 = NodeTable.insert_node(
            node_uri=str(node_uri4),
            c=db_cursor,
        )
        node4 = NodeTable.fetch_node(node_key=node_key4, c=db_cursor)
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup node tag table
        NodeTagTable.create_node_tag_table(db_cursor)
        # Link n1 - t1
        NodeTagTable.insert_link(
            node_key=node_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link n1 - t2
        NodeTagTable.insert_link(
            node_key=node_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link n2 - t1
        NodeTagTable.insert_link(
            node_key=node_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link n3 - t2
        NodeTagTable.insert_link(
            node_key=node_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Check tags linked to node1
        tags = NodeTagTable.get_linked_tags(node_key=node_key1, c=db_cursor)
        assert tag1 in tags
        assert tag2 in tags
        assert len(tags) == 2
        # Check tags linked to node2
        tags = NodeTagTable.get_linked_tags(node_key=node_key2, c=db_cursor)
        assert tag1 in tags
        assert tag2 not in tags
        assert len(tags) == 1
        # Check tags linked to node3
        tags = NodeTagTable.get_linked_tags(node_key=node_key3, c=db_cursor)
        assert tag2 in tags
        assert tag1 not in tags
        assert len(tags) == 1
        # Check tags linked to node4
        tags = NodeTagTable.get_linked_tags(node_key=node_key4, c=db_cursor)
        assert len(tags) == 0

        # Check nodes linked to tag1
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag1', c=db_cursor)
        assert node1 in nodes
        assert node2 in nodes
        assert len(nodes) == 2
        # Check nodes linked to tag2
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag2', c=db_cursor)
        assert node1 in nodes
        assert node3 in nodes
        assert len(nodes) == 2
        # Check nodes linked to tag3
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag3', c=db_cursor)
        assert len(nodes) == 0

        # Delete link n1 and t1
        NodeTagTable.delete_link(node_key=node_key1, tag_name='tag1', c=db_cursor)
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag1', c=db_cursor)
        assert len(nodes) == 1
        assert node2 in nodes
        tags = NodeTagTable.get_linked_tags(node_key=node_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag2 in tags

        # Delete link n3 and t2
        NodeTagTable.delete_link(node_key=node_key3, tag_name='tag2', c=db_cursor)
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag2', c=db_cursor)
        assert node1 in nodes
        assert len(nodes) == 1
        tags = NodeTagTable.get_linked_tags(node_key=node_key3, c=db_cursor)
        assert len(tags) == 0
        return

    @staticmethod
    def test_delete_propagation(db_cursor, node_uri, node_uri2, node_uri3, node_uri4):
        # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key1, db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key2, db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key3, db_cursor)
        node_key4 = NodeTable.insert_node(
            node_uri=str(node_uri4),
            c=db_cursor,
        )
        node4 = NodeTable.fetch_node(node_key4, db_cursor)
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup node tag table
        NodeTagTable.create_node_tag_table(db_cursor)
        # Link ,1 - t1
        NodeTagTable.insert_link(
            node_key=node_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link n1 - t2
        NodeTagTable.insert_link(
            node_key=node_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link n2 - t1
        NodeTagTable.insert_link(
            node_key=node_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link n3 - t2
        NodeTagTable.insert_link(
            node_key=node_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Delete node2 from Node table
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag1', c=db_cursor)
        assert len(nodes) == 2
        assert node1 in nodes
        assert node2 in nodes
        NodeTable.delete_node(node_key=node_key2, c=db_cursor)
        nodes = NodeTagTable.get_linked_nodes(tag_name='tag1', c=db_cursor)
        assert len(nodes) == 1
        assert node1 in nodes

        # Delete tag2 from Tag table
        tags = NodeTagTable.get_linked_tags(node_key=node_key1, c=db_cursor)
        assert len(tags) == 2
        assert tag1 in tags
        assert tag2 in tags
        TagTable.delete_tag(name='tag2', c=db_cursor)
        tags = NodeTagTable.get_linked_tags(node_key=node_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag1 in tags
        assert tag2 not in tags
        return


class TestExpressionTagTable:

    @staticmethod
    def test_create_expression_tag_table(db_cursor):
        # Check if the table 'expression_tag' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expression_tag';")
        assert db_cursor.fetchone() is None

        # Create
        ExpressionTagTable.create_expression_tag_table(db_cursor)
        # Check if the table 'expression_tag' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='expression_tag';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'expression_tag' is empty
        db_cursor.execute("SELECT COUNT(*) FROM expression_tag;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return
    
    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, expression_uri, expression_uri2, expression_uri3, expression_uri4):
        # Set up expression table
        ExpressionTable.create_expression_table(c=db_cursor)
        # Create 4 expressions
        expression_key1 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor,
        )
        expression1 = ExpressionTable.fetch_expression(expression_key=expression_key1, c=db_cursor)
        expression_key2 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri2),
            c=db_cursor,
        )
        expression2 = ExpressionTable.fetch_expression(expression_key=expression_key2, c=db_cursor)
        expression_key3 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri3),
            c=db_cursor,
        )
        expression3 = ExpressionTable.fetch_expression(expression_key=expression_key3, c=db_cursor)
        expression_key4 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri4),
            c=db_cursor,
        )
        expression4 = ExpressionTable.fetch_expression(expression_key=expression_key4, c=db_cursor)
        
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup expression tag table
        ExpressionTagTable.create_expression_tag_table(c=db_cursor)
        # Link e1 - t1
        ExpressionTagTable.insert_link(
            expression_key=expression_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link e1 - t2
        ExpressionTagTable.insert_link(
            expression_key=expression_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link e2 - t1
        ExpressionTagTable.insert_link(
            expression_key=expression_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link e3 - t2
        ExpressionTagTable.insert_link(
            expression_key=expression_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Check tags linked to expression1
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key1, c=db_cursor)
        assert tag1 in tags
        assert tag2 in tags
        assert len(tags) == 2
        # Check tags linked to expression2
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key2, c=db_cursor)
        assert tag1 in tags
        assert tag2 not in tags
        assert len(tags) == 1
        # Check tags linked to expression3
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key3, c=db_cursor)
        assert tag2 in tags
        assert tag1 not in tags
        assert len(tags) == 1
        # Check tags linked to expression4
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key4, c=db_cursor)
        assert len(tags) == 0

        # Check expressions linked to tag1
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag1', c=db_cursor)
        assert expression1 in expressions
        assert expression2 in expressions
        assert len(expressions) == 2
        # Check expressions linked to tag2
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag2', c=db_cursor)
        assert expression1 in expressions
        assert expression3 in expressions
        assert len(expressions) == 2
        # Check expressions linked to tag3
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag3', c=db_cursor)
        assert len(expressions) == 0

        # Delete link e1 and t1
        ExpressionTagTable.delete_link(expression_key=expression_key1, tag_name='tag1', c=db_cursor)
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag1', c=db_cursor)
        assert len(expressions) == 1
        assert expression2 in expressions
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag2 in tags

        # Delete link e3 and t2
        ExpressionTagTable.delete_link(expression_key=expression_key3, tag_name='tag2', c=db_cursor)
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag2', c=db_cursor)
        assert expression1 in expressions
        assert len(expressions) == 1
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key3, c=db_cursor)
        assert len(tags) == 0
        return

    @staticmethod
    def test_delete_propagation(db_cursor, expression_uri, expression_uri2, expression_uri3, expression_uri4):
        # Set up expression table
        ExpressionTable.create_expression_table(c=db_cursor)
        # Create 4 expressions
        expression_key1 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor,
        )
        expression1 = ExpressionTable.fetch_expression(expression_key=expression_key1, c=db_cursor)
        expression_key2 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri2),
            c=db_cursor,
        )
        expression2 = ExpressionTable.fetch_expression(expression_key=expression_key2, c=db_cursor)
        expression_key3 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri3),
            c=db_cursor,
        )
        expression3 = ExpressionTable.fetch_expression(expression_key=expression_key3, c=db_cursor)
        expression_key4 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri4),
            c=db_cursor,
        )
        expression4 = ExpressionTable.fetch_expression(expression_key=expression_key4, c=db_cursor)
        # Set up tag table
        TagTable.create_tag_table(c=db_cursor)
        # Create 3 tags
        TagTable.insert_tag(name='tag1', c=db_cursor)
        tag1 = TagTable.fetch_tag(name='tag1', c=db_cursor)
        TagTable.insert_tag(name='tag2', c=db_cursor)
        tag2 = TagTable.fetch_tag(name='tag2', c=db_cursor)
        TagTable.insert_tag(name='tag3', c=db_cursor)
        tag3 = TagTable.fetch_tag(name='tag3', c=db_cursor)
        # Setup expression tag table
        ExpressionTagTable.create_expression_tag_table(db_cursor)
        # Link e1 - t1
        ExpressionTagTable.insert_link(
            expression_key=expression_key1,
            tag_name='tag1',
            c=db_cursor)
        # Link e1 - t2
        ExpressionTagTable.insert_link(
            expression_key=expression_key1,
            tag_name='tag2',
            c=db_cursor)
        # Link e2 - t1
        ExpressionTagTable.insert_link(
            expression_key=expression_key2,
            tag_name='tag1',
            c=db_cursor)
        # Link e3 - t2
        ExpressionTagTable.insert_link(
            expression_key=expression_key3,
            tag_name='tag2',
            c=db_cursor)
 
        # Delete expression2 from Expression table
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag1', c=db_cursor)
        assert len(expressions) == 2
        assert expression1 in expressions
        assert expression2 in expressions
        ExpressionTable.delete_expression(expression_key=expression_key2, c=db_cursor)
        expressions = ExpressionTagTable.get_linked_expressions(tag_name='tag1', c=db_cursor)
        assert len(expressions) == 1
        assert expression1 in expressions

        # Delete tag2 from Tag table
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key1, c=db_cursor)
        assert len(tags) == 2
        assert tag1 in tags
        assert tag2 in tags
        TagTable.delete_tag(name='tag2', c=db_cursor)
        tags = ExpressionTagTable.get_linked_tags(expression_key=expression_key1, c=db_cursor)
        assert len(tags) == 1
        assert tag1 in tags
        assert tag2 not in tags
        return

class TestRecordNodeTable:

    @staticmethod
    def test_create_record_node_table(db_cursor):
        # Check if the table 'record_node' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_node';")
        assert db_cursor.fetchone() is None

        # Create
        RecordNodeTable.create_record_node_table(db_cursor)
        # Check if the table 'record_node' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='record_node';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'record_node' is empty
        db_cursor.execute("SELECT COUNT(*) FROM record_node;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return

    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, text_uri, node_uri, node_uri2, node_uri3, node_uri4):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 4 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)
        record_key4 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref4',
            c=db_cursor,
        )
        record4 = RecordTable.fetch_record(record_key4, db_cursor)
        # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key1, db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key2, db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key3, db_cursor)
        # Setup record node table
        RecordNodeTable.create_record_node_table(db_cursor)
        # Link r1 - n1
        RecordNodeTable.insert_link(
            record_key=record_key1,
            node_key=node_key1,
            c=db_cursor)
        # Link r1 - n2
        RecordNodeTable.insert_link(
            record_key=record_key1,
            node_key=node_key2,
            c=db_cursor)
        # Link r2 - n1
        RecordNodeTable.insert_link(
            record_key=record_key2,
            node_key=node_key1,
            c=db_cursor)
        # Link r3 - n2
        RecordNodeTable.insert_link(
            record_key=record_key3,
            node_key=node_key2,
            c=db_cursor)
 
        # Check nodes linked to record1
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key1, c=db_cursor)
        assert node1 in nodes
        assert node2 in nodes
        assert len(nodes) == 2
        # Check nodes linked to record2
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key2, c=db_cursor)
        assert node1 in nodes
        assert node2 not in nodes
        assert len(nodes) == 1
        # Check nodes linked to record3
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key3, c=db_cursor)
        assert node2 in nodes
        assert node1 not in nodes
        assert len(nodes) == 1
        # Check nodes linked to record4
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key4, c=db_cursor)
        assert len(nodes) == 0

        # Check records linked to node1
        records = RecordNodeTable.get_linked_records(node_key=node_key1, c=db_cursor)
        assert record1 in records
        assert record2 in records
        assert len(records) == 2
        # Check records linked to node2
        records = RecordNodeTable.get_linked_records(node_key=node_key2, c=db_cursor)
        assert record1 in records
        assert record3 in records
        assert len(records) == 2
        # Check records linked to node3
        records = RecordNodeTable.get_linked_records(node_key=node_key3, c=db_cursor)
        assert len(records) == 0

        # Delete link r1 and n1
        RecordNodeTable.delete_link(record_key=record_key1, node_key=node_key1, c=db_cursor)
        records = RecordNodeTable.get_linked_records(node_key=node_key1, c=db_cursor)
        assert len(records) == 1
        assert record2 in records
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key1, c=db_cursor)
        assert len(nodes) == 1
        assert node2 in nodes

        # Delete link r3 and n2
        RecordNodeTable.delete_link(record_key=record_key3, node_key=node_key2, c=db_cursor)
        records = RecordNodeTable.get_linked_records(node_key=node_key2, c=db_cursor)
        assert record1 in records
        assert len(records) == 1
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key3, c=db_cursor)
        assert len(nodes) == 0
        return

    @staticmethod
    def test_delete_propagation(db_cursor, text_uri, node_uri, node_uri2, node_uri3, node_uri4):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
        # Create 4 records
        record_key1 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref1',
            c=db_cursor,
        )
        record1 = RecordTable.fetch_record(record_key1, db_cursor)
        record_key2 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref2',
            c=db_cursor,
        )
        record2 = RecordTable.fetch_record(record_key2, db_cursor)
        record_key3 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref3',
            c=db_cursor,
        )
        record3 = RecordTable.fetch_record(record_key3, db_cursor)
        record_key4 = RecordTable.insert_record(
            text_uri=str(text_uri),
            image_uri='',
            sound_uri='',
            reference='ref4',
            c=db_cursor,
        )
        record4 = RecordTable.fetch_record(record_key4, db_cursor)
        # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key1, db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key2, db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key3, db_cursor)
        # Setup record node table
        RecordNodeTable.create_record_node_table(db_cursor)
        # Link r1 - n1
        RecordNodeTable.insert_link(
            record_key=record_key1,
            node_key=node_key1,
            c=db_cursor)
        # Link r1 - n2
        RecordNodeTable.insert_link(
            record_key=record_key1,
            node_key=node_key2,
            c=db_cursor)
        # Link r2 - n1
        RecordNodeTable.insert_link(
            record_key=record_key2,
            node_key=node_key1,
            c=db_cursor)
        # Link r3 - n2
        RecordNodeTable.insert_link(
            record_key=record_key3,
            node_key=node_key2,
            c=db_cursor)

        # Delete record2 from Record table
        records = RecordNodeTable.get_linked_records(node_key=node_key1, c=db_cursor)
        assert len(records) == 2
        assert record1 in records
        assert record2 in records
        RecordTable.delete_record(record_key=record_key2, c=db_cursor)
        records = RecordNodeTable.get_linked_records(node_key=node_key1, c=db_cursor)
        assert len(records) == 1
        assert record1 in records

        # Delete node2 from Node table
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key1, c=db_cursor)
        assert len(nodes) == 2
        assert node1 in nodes
        assert node2 in nodes
        NodeTable.delete_node(node_key=node_key2, c=db_cursor)
        nodes = RecordNodeTable.get_linked_nodes(record_key=record_key1, c=db_cursor)
        assert len(nodes) == 1
        assert node1 in nodes
        assert node2 not in nodes
        return

class TestNodeExpressionTable:

    @staticmethod
    def test_create_node_expression_table(db_cursor):
        # Check if the table 'node_expression' does not exist yet
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node_expression';")
        assert db_cursor.fetchone() is None

        # Create
        NodeExpressionTable.create_node_expression_table(db_cursor)
        # Check if the table 'node_expression' is created
        db_cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='node_expression';")
        assert db_cursor.fetchone() is not None
        
        # Check if the table 'node_expression' is empty
        db_cursor.execute("SELECT COUNT(*) FROM node_expression;")
        row_count = db_cursor.fetchone()[0]
        assert row_count == 0
        return
    
    @staticmethod
    def test_insert_fetch_delete_link(db_cursor, node_uri, node_uri2, node_uri3, node_uri4, expression_uri, expression_uri2, expression_uri3):
        # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key1, db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key2, db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key3, db_cursor)
        node_key4 = NodeTable.insert_node(
            node_uri=str(node_uri4),
            c=db_cursor,
        )
        node4 = NodeTable.fetch_node(node_key4, db_cursor)
        # Set up expression table
        ExpressionTable.create_expression_table(c=db_cursor)
        # Create 3 expressions
        expression_key1 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor,
        )
        expression1 = ExpressionTable.fetch_expression(expression_key=expression_key1, c=db_cursor)
        expression_key2 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri2),
            c=db_cursor,
        )
        expression2 = ExpressionTable.fetch_expression(expression_key=expression_key2, c=db_cursor)
        expression_key3 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri3),
            c=db_cursor,
        )
        expression3 = ExpressionTable.fetch_expression(expression_key=expression_key3, c=db_cursor)
        # Setup node expression table
        NodeExpressionTable.create_node_expression_table(db_cursor)
        # Link n1 - e1
        NodeExpressionTable.insert_link(
            node_key=node_key1,
            expression_key=expression_key1,
            c=db_cursor)
        # Link n1 - e2
        NodeExpressionTable.insert_link(
            node_key=node_key1,
            expression_key=expression_key2,
            c=db_cursor)
        # Link n2 - e1
        NodeExpressionTable.insert_link(
            node_key=node_key2,
            expression_key=expression_key1,
            c=db_cursor)
        # Link n3 - e2
        NodeExpressionTable.insert_link(
            node_key=node_key3,
            expression_key=expression_key2,
            c=db_cursor)
 
        # Check expressions linked to node1
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key1, c=db_cursor)
        assert expression1 in expressions
        assert expression2 in expressions
        assert len(expressions) == 2
        # Check expressions linked to node2
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key2, c=db_cursor)
        assert expression1 in expressions
        assert expression2 not in expressions
        assert len(expressions) == 1
        # Check expressions linked to node3
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key3, c=db_cursor)
        assert expression2 in expressions
        assert expression1 not in expressions
        assert len(expressions) == 1
        # Check expressions linked to node4
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key4, c=db_cursor)
        assert len(expressions) == 0

        # Check nodes linked to expression1
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key1, c=db_cursor)
        assert node1 in nodes
        assert node2 in nodes
        assert len(nodes) == 2
        # Check nodes linked to expression2
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key2, c=db_cursor)
        assert node1 in nodes
        assert node3 in nodes
        assert len(nodes) == 2
        # Check nodes linked to expression3
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key3, c=db_cursor)
        assert len(nodes) == 0

        # Delete link n1 and e1
        NodeExpressionTable.delete_link(node_key=node_key1, expression_key=expression_key1, c=db_cursor)
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key1, c=db_cursor)
        assert len(nodes) == 1
        assert node2 in nodes
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key1, c=db_cursor)
        assert len(expressions) == 1
        assert expression2 in expressions

        # Delete link n3 and e2
        NodeExpressionTable.delete_link(node_key=node_key3, expression_key=expression_key2, c=db_cursor)
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key2, c=db_cursor)
        assert node1 in nodes
        assert len(nodes) == 1
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key3, c=db_cursor)
        assert len(expressions) == 0
        return

    @staticmethod
    def test_delete_propagation(db_cursor, node_uri, node_uri2, node_uri3, node_uri4, expression_uri, expression_uri2, expression_uri3):
        
                # Set up node table
        NodeTable.create_node_table(c=db_cursor)
        # Create 4 nodes
        node_key1 = NodeTable.insert_node(
            node_uri=str(node_uri),
            c=db_cursor,
        )
        node1 = NodeTable.fetch_node(node_key1, db_cursor)
        node_key2 = NodeTable.insert_node(
            node_uri=str(node_uri2),
            c=db_cursor,
        )
        node2 = NodeTable.fetch_node(node_key2, db_cursor)
        node_key3 = NodeTable.insert_node(
            node_uri=str(node_uri3),
            c=db_cursor,
        )
        node3 = NodeTable.fetch_node(node_key3, db_cursor)
        node_key4 = NodeTable.insert_node(
            node_uri=str(node_uri4),
            c=db_cursor,
        )
        node4 = NodeTable.fetch_node(node_key4, db_cursor)
        # Set up expression table
        ExpressionTable.create_expression_table(c=db_cursor)
        # Create 3 expressions
        expression_key1 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri),
            c=db_cursor,
        )
        expression1 = ExpressionTable.fetch_expression(expression_key=expression_key1, c=db_cursor)
        expression_key2 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri2),
            c=db_cursor,
        )
        expression2 = ExpressionTable.fetch_expression(expression_key=expression_key2, c=db_cursor)
        expression_key3 = ExpressionTable.insert_expression(
            expression_uri=str(expression_uri3),
            c=db_cursor,
        )
        expression3 = ExpressionTable.fetch_expression(expression_key=expression_key3, c=db_cursor)
        # Setup node expression table
        NodeExpressionTable.create_node_expression_table(db_cursor)
        # Link n1 - e1
        NodeExpressionTable.insert_link(
            node_key=node_key1,
            expression_key=expression_key1,
            c=db_cursor)
        # Link n1 - e2
        NodeExpressionTable.insert_link(
            node_key=node_key1,
            expression_key=expression_key2,
            c=db_cursor)
        # Link n2 - e1
        NodeExpressionTable.insert_link(
            node_key=node_key2,
            expression_key=expression_key1,
            c=db_cursor)
        # Link n3 - e2
        NodeExpressionTable.insert_link(
            node_key=node_key3,
            expression_key=expression_key2,
            c=db_cursor)

        # Delete node2 from Node table
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key1, c=db_cursor)
        assert len(nodes) == 2
        assert node1 in nodes
        assert node2 in nodes
        NodeTable.delete_node(node_key=node_key2, c=db_cursor)
        nodes = NodeExpressionTable.get_linked_nodes(expression_key=expression_key1, c=db_cursor)
        assert len(nodes) == 1
        assert node1 in nodes

        # Delete expression2 from Expression table
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key1, c=db_cursor)
        assert len(expressions) == 2
        assert expression1 in expressions
        assert expression2 in expressions
        ExpressionTable.delete_expression(expression_key=expression_key2, c=db_cursor)
        expressions = NodeExpressionTable.get_linked_expressions(node_key=node_key1, c=db_cursor)
        assert len(expressions) == 1
        assert expression1 in expressions
        assert expression2 not in expressions
        return

class TestSQLiteDatabase:

    @staticmethod
    def test(tmp_path_factory):
        fpath_db: pathlib.Path = tmp_path_factory.mktemp("database") / "test.db"
        db = SQLiteDatabase(fpath_db=fpath_db, overwrite=True)
        tag = db.insert_tag(tag_name='tag1')
        Tag = db.fetch_tag(key=tag)
        
