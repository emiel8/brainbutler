import typing as t

from ..interface.database_adapter import DatabaseAdapter
from ..components.record import Record
from ..components.record import Tag


import os
import sqlite3
import uuid

def fill_database(fpath_db: str):
    conn = sqlite3.connect(fpath_db)
    c = conn.cursor()

    record_id1 = RecordTable.insert_record(text_uri='test/uri1', image_uri='', sound_uri='', reference='ref1', c=c)
    record_id2 = RecordTable.insert_record(text_uri='test/uri2', image_uri='test/image2', sound_uri='', reference='ref2', c=c)

    tag1 = TagTable.insert_tag(name='tag1', c=c)

    node_id1 = NodeTable.insert_node(uri_node='node1', c=c)

    expression_id1 = ExpressionTable.insert_expression(uri_expression='expression1', c=c)

    # Add links
    RecordTagTable.insert_link(record_id=record_id1, tag_name=tag1, c=c)
    conn.commit()
    return

class RecordTable:

    @staticmethod
    def create_record_table(c: sqlite3.Cursor):
        # Create the table
        create_table_query = '''
        CREATE TABLE record (
            id TEXT PRIMARY KEY,
            text_uri TEXT,
            image_uri TEXT,
            sound_uri TEXT,
            reference TEXT
        );
        '''
        c.execute(create_table_query)
        return

    @staticmethod
    def insert_record(
        text_uri: str,
        image_uri: str,
        sound_uri: str,
        reference: str,
        c: sqlite3.Cursor):
        # Generate a UUID for the new record
        record_key = str(uuid.uuid4())
        
        # Insert the record into the table
        insert_query = '''
        INSERT INTO record (id, text_uri, image_uri, sound_uri, reference)
        VALUES (?, ?, ?, ?, ?);
        '''
        c.execute(insert_query, (record_key, text_uri, image_uri, sound_uri, reference))
        return record_key

    @staticmethod
    def fetch_record(
        record_key: str,
        c: sqlite3.Cursor) -> Record:
        """ Fetch the given record from the record table.

        :param record_key: Key of the record to fetch.
        :param c: Cursor of the database to use.

        :return: The record.
        """
        fetch_record_query = 'SELECT * FROM record WHERE id = ?'
        c.execute(fetch_record_query, (record_key))
        return Record(c.fetchone())
    
    @staticmethod
    def update_record(
        record_key: str,
        c: sqlite3.Cursor,
        text_uri: t.Optional[str] = None,
        image_uri: t.Optional[str] = None,
        sound_uri: t.Optional[str] = None,
        reference: t.Optional[str] = None,
        ):
        # Create the update query with only the fields that are not None
        update_query = '''
        UPDATE record
        SET text_uri = COALESCE(?, text_uri),
            image_uri = COALESCE(?, image_uri),
            sound_uri = COALESCE(?, sound_uri),
            reference = COALESCE(?, reference)
        WHERE id = ?;
        '''
        c.execute(update_query, (text_uri, image_uri, sound_uri, reference, record_key))
        return

    @staticmethod
    def delete_record(c: sqlite3.Cursor, record_key: str):
        delete_record_query = 'DELETE FROM record WHERE id = ?;'
        c.execute(delete_record_query, (record_key,))
        return

class TagTable:

    @staticmethod
    def create_tag_table(c: sqlite3.Cursor):
        # Create the table
        create_tag_query = '''
        CREATE TABLE tag (
            name TEXT PRIMARY KEY
        );
        '''
        c.execute(create_tag_query)
        return
    
    @staticmethod
    def insert_tag(
        name: str,
        c: sqlite3.Cursor):
        insert_query = '''
        INSERT INTO tag (name) VALUES (?);
        '''
        c.execute(insert_query, (name,))
        return name
    
    @staticmethod
    def fetch_tag(
        name: str,
        c: sqlite3.Cursor,
        ) -> Tag:
        fetch_query = 'SELECT * FROM tag WHERE id = ?'
        c.execute(fetch_query, (name,))
        return Tag(c.fetchone())

    @staticmethod
    def update_tag(old_name: str, new_name: str, c: sqlite3.Cursor):
        update_query = '''
        UPDATE tag
        SET name = ?
        WHERE name = ?;
        '''
        c.execute(update_query, (new_name, old_name))
        return
    
    @staticmethod
    def delete_tag(name: str, c: sqlite3.Cursor):
        delete_query = '''
        DELETE FROM tag
        WHERE name = ?;
        '''
        c.execute(delete_query, (name,))
        return


class NodeTable:

    @staticmethod
    def create_node_table(c: sqlite3.Cursor):
        # Create the table
        create_table_query = '''
        CREATE TABLE node (
            id TEXT PRIMARY KEY,
            uri_node TEXT
        );
        '''
        c.execute(create_table_query)
        return
    
    @staticmethod
    def insert_node(
        uri_node: str,
        c: sqlite3.Cursor) -> str:
        insert_query = '''
        INSERT INTO node (id, uri_node) VALUES (?, ?);
        '''
        node_id = str(uuid.uuid4())
        c.execute(insert_query, (node_id, uri_node))
        return node_id

class ExpressionTable:

    @staticmethod
    def create_expression_table(c: sqlite3.Cursor):
        # Create the table
        create_table_query = '''
        CREATE TABLE expression (
            id TEXT PRIMARY KEY,
            uri_expression TEXT
        );
        '''
        c.execute(create_table_query)
        return
    
    @staticmethod
    def insert_expression(
        uri_expression: str,
        c: sqlite3.Cursor
        ) -> str:
        insert_query = '''
        INSERT INTO expression (id, uri_expression) VALUES (?, ?);
        '''
        expression_id = str(uuid.uuid4())
        c.execute(insert_query, (expression_id, uri_expression))
        return expression_id

class RecordRecordTable:

    @staticmethod
    def create_record_record_table(c: sqlite3.Cursor):
        create_table_query = '''
        CREATE TABLE record_links (
            record_id1 TEXT NOT NULL,
            record_id2 TEXT NOT NULL,
            PRIMARY KEY (record_id1, record_id2),
            FOREIGN KEY (record_id1) REFERENCES record (id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (record_id2) REFERENCES record (id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        '''
        c.execute(create_table_query)
        return

class RecordTagTable:

    @staticmethod
    def create_record_tag_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE record_tag (
            record_id TEXT,
            tag_name TEXT,
            FOREIGN KEY (record_id) REFERENCES record(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES tag(name) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (record_id, tag_name)
        );
        '''
        c.execute(create_table_query)
        return
    
    @staticmethod
    def insert_link(
        record_id: str,
        tag_name: str,
        c: sqlite3.Cursor):
        insert_link_query = '''
        INSERT INTO record_tag (record_id, tag_name) VALUES (?, ?);
        '''
        c.execute(insert_link_query, (record_id, tag_name))
        return

class NodeTagTable:

    @staticmethod
    def create_node_tag_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE node_tag (
            node_id TEXT,
            tag_name TEXT,
            FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES tag(name) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (node_id, tag_name)
        );
        '''
        c.execute(create_table_query)
        return

class ExpressionTagTable:

    @staticmethod
    def create_expression_tag_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE expression_tag (
            expression_id TEXT,
            tag_name TEXT,
            FOREIGN KEY (expression_id) REFERENCES expression(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES tag(name) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (expression_id, tag_name)
        );
        '''
        c.execute(create_table_query)
        return

class RecordNodeTable:

    @staticmethod
    def create_record_node_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE record_node (
            record_id TEXT,
            node_id TEXT,
            FOREIGN KEY (record_id) REFERENCES record(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (record_id, node_id)
        );
        '''
        c.execute(create_table_query)
        return

class NodeExpressionTable:

    @staticmethod
    def create_node_expression_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE node_expression (
            node_id TEXT,
            expression_id TEXT,
            FOREIGN KEY (node_id) REFERENCES node(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (expression_id) REFERENCES expression(id) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (node_id, expression_id)
        );
        '''
        c.execute(create_table_query)
        return



class SQLiteDatabase(DatabaseAdapter):

    @staticmethod
    def initialize_database(fpath_db: str, overwrite: bool = False):
        """ Initialize the SQLite Database

        :param fpath_db: Filepath where the sqlite database will be stored.
        :param overwrite: If True, overwrite when database at given path already exists. Otherwise do nothing.
        """
        if os.path.isfile(fpath_db) and not overwrite:
            return
        conn = sqlite3.connect(fpath_db)
        c = conn.cursor()

        # Create entity tables
        RecordTable.create_record_table(c=c)
        TagTable.create_tag_table(c=c)
        NodeTable.create_node_table(c=c)
        ExpressionTable.create_expression_table(c=c)

        # Create relation tables
        RecordRecordTable.create_record_record_table(c=c)
        RecordTagTable.create_record_tag_table(c=c)
        RecordNodeTable.create_record_node_table(c=c)
        NodeExpressionTable.create_node_expression_table(c=c)
        NodeTagTable.create_node_tag_table(c=c)
        ExpressionTagTable.create_expression_tag_table(c=c)

        conn.commit()
        conn.close()
        return