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
def expression_uri() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression.json')

@pytest.fixture(scope='module')
def expression_uri2() -> AnyUrl:
    return AnyUrl('c:/Users/myexpression2.json')


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
    def test_insert_fetch_delete_link(db_cursor, text_uri):
        # Set up record table
        RecordTable.create_record_table(c=db_cursor)
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
        RecordRecordTable.create_record_record_table(db_cursor)
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
        print('hell')
        db_cursor.execute("SELECT * FROM record_links")
        print(db_cursor.fetchall())
        records = RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)
        assert len(records) == 1
        assert record3 in records
        assert record2 not in records
        assert record1 not in records

        # Check if delete link works
        RecordRecordTable.delete_link(record_key1=record_key1, record_key2=record_key3, c=db_cursor)
        print(record_key1)
        print(record_key2)
        print(record_key3)
        # Check if the table 'record_links' is empty
        db_cursor.execute("SELECT * FROM record_links")
        print(db_cursor.fetchall())
        print('\n\n')
        db_cursor.execute("SELECT COUNT(*) FROM record_links;")
        row_count = db_cursor.fetchone()[0]
        print(row_count)
        # TODO fix
        assert row_count == 0

        assert len(RecordRecordTable.fetch_linked_records(record_key=record_key1, c=db_cursor)) == 0
        