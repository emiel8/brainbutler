import typing as t

from ..interface.database_adapter import DatabaseAdapter
from ..components.record import Record
from ..components.tag import Tag
from ..components.node import Node
from ..components.expression import Expression


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
        """
        :param c: Database cursor.
        """
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
        c: sqlite3.Cursor) -> str:
        """
        :param text_uri: Text URI.
        :param image_uri: Image URI.
        :param sound_uri: Sound URI.
        :param reference: Reference of the record.
        :param c: Database cursor.

        :return: Key of the record.
        """
        # Generate a UUID for the new record
        record_key = str(uuid.uuid4())
        
        # Insert the record into the table
        insert_query = '''
        INSERT INTO record (id, text_uri, image_uri, sound_uri, reference)
        VALUES (?, ?, ?, ?, ?);
        '''
        c.execute(insert_query, (record_key, text_uri, image_uri, sound_uri, reference,))
        return record_key

    @staticmethod
    def fetch_record(
        record_key: str,
        c: sqlite3.Cursor) -> t.Union[Record, None]:
        """ Fetch the given record from the record table.

        :param record_key: Key of the record to fetch.
        :param c: Cursor of the database to use.

        :return: The record when found, None otherwise.
        """
        fetch_record_query = 'SELECT * FROM record WHERE id = ?'
        c.execute(fetch_record_query, (record_key,))
        record = c.fetchone()
        if record:
            return Record(
                uuid=record[0],
                text_uri=record[1],
                image_uri=record[2],
                sound_uri=record[3],
                reference=record[4])
        return None
    
    @staticmethod
    def update_record(
        record_key: str,
        c: sqlite3.Cursor,
        text_uri: t.Optional[str] = None,
        image_uri: t.Optional[str] = None,
        sound_uri: t.Optional[str] = None,
        reference: t.Optional[str] = None,
        ):
        """
        :param record_key: Key of the record to update.
        :param c: Database cursor.
        :param text_uri: New text URI.
        :param image_uri: New image URI.
        :param sound_uri: New sound URI.
        :param reference: New reference.
        """
        # Create the update query with only the fields that are not None
        update_query = '''
        UPDATE record
        SET text_uri = COALESCE(?, text_uri),
            image_uri = COALESCE(?, image_uri),
            sound_uri = COALESCE(?, sound_uri),
            reference = COALESCE(?, reference)
        WHERE id = ?;
        '''
        c.execute(update_query, (text_uri, image_uri, sound_uri, reference, record_key,))
        return

    @staticmethod
    def delete_record(record_key: str, c: sqlite3.Cursor):
        """
        :param record_key: Key of the record to delete.
        :param c: Database cursor.
        """
        delete_record_query = 'DELETE FROM record WHERE id = ?;'
        c.execute(delete_record_query, (record_key,))
        return

class TagTable:

    @staticmethod
    def create_tag_table(c: sqlite3.Cursor):
        """
        :param c: Database cursor.
        """
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
        c: sqlite3.Cursor) -> str:
        """
        :param name: Name of the tag.
        :param c: Database cursor.

        :return: Name of the tag.
        """
        insert_query = '''
        INSERT INTO tag (name) VALUES (?);
        '''
        c.execute(insert_query, (name,))
        return name
    
    @staticmethod
    def fetch_tag(
        name: str,
        c: sqlite3.Cursor,
        ) -> t.Union[Tag, None]:
        """
        :param name: Name of the tag.
        :param c: Database cursor.

        :return: Tag if found, None otherwise.
        """
        fetch_query = 'SELECT * FROM tag WHERE name = ?'
        c.execute(fetch_query, (name,))
        tag = c.fetchone()
        if tag:
            return Tag(name=tag[0])
        return None

    @staticmethod
    def update_tag(old_name: str, new_name: str, c: sqlite3.Cursor):
        """
        :param old_name: Old name of the tag.
        :param new_name: New name of the tag.
        :param c: Database cursor.
        """
        update_query = '''
        UPDATE tag
        SET name = ?
        WHERE name = ?;
        '''
        c.execute(update_query, (new_name, old_name))
        return
    
    @staticmethod
    def delete_tag(name: str, c: sqlite3.Cursor):
        """
        :param name: Name of the tag to delete.
        :param c: Database cursor.
        """
        delete_query = '''
        DELETE FROM tag
        WHERE name = ?;
        '''
        c.execute(delete_query, (name,))
        return


class NodeTable:

    @staticmethod
    def create_node_table(c: sqlite3.Cursor):
        """
        :param c: Database cursor.
        """
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
        node_uri: str,
        c: sqlite3.Cursor) -> str:
        """
        :param node_uri: URI of the node to add.
        :param c: Database cursor.

        :return: Key of the created node.
        """
        insert_query = '''
        INSERT INTO node (id, uri_node) VALUES (?, ?);
        '''
        node_key = str(uuid.uuid4())
        c.execute(insert_query, (node_key, node_uri))
        return node_key
    
    @staticmethod
    def fetch_node(
            node_key: str,
            c: sqlite3.Cursor,
            ) -> t.Union[Node, None]:
        """
        :param node_key: Key of the node to fetch.
        :param c: Database cursor.

        :return: Node object if node found, None otherwise. 
        """
        fetch_query = 'SELECT * FROM node where id = ?;'
        c.execute(fetch_query, (node_key,))
        node = c.fetchone()
        if node:
            return Node(uuid=node[0], node_uri=node[1])
        return None

    @staticmethod
    def update_node(
            node_key: str,
            node_uri: t.Optional[str],
            c: sqlite3.Cursor):
        """
        :param node_key: Key of the node to update.
        :param node_uri: New URI for the node.
        :param c: Database cursor.
        """
        update_query = '''
        UPDATE node
        SET uri_node = COALESCE(?, uri_node)
        WHERE id = ?;
        '''
        c.execute(update_query, (node_uri, node_key,))
        return

    @staticmethod
    def delete_node(node_key: str, c: sqlite3.Cursor):
        """
        :param node_key: Key of the node to delete.
        :param c: Database cursor.
        """
        delete_query = '''
        DELETE FROM node
        WHERE id = ?;
        '''
        c.execute(delete_query, (node_key,))
        return

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
        expression_uri: str,
        c: sqlite3.Cursor
        ) -> str:
        """
        :param expression_uri: URI of the expression.
        :param c: Database cursor.

        :return: Key of the expression.
        """
        insert_query = '''
        INSERT INTO expression (id, uri_expression) VALUES (?, ?);
        '''
        expression_key = str(uuid.uuid4())
        c.execute(insert_query, (expression_key, expression_uri,))
        return expression_key
    
    @staticmethod
    def fetch_expression(
        expression_key: str,
        c: sqlite3.Cursor
        ) -> t.Union[Expression, None]:
        """
        :param expression_key: Key of the expression to fetch.
        :param c: Database cursor.

        :return: Expression if found, None otherwise.
        """
        fetch_query = 'SELECT * FROM expression WHERE id = ?;'
        c.execute(fetch_query, (expression_key,))
        expression = c.fetchone()
        if expression:
            return Expression(
                uuid=expression[0],
                expression_uri=expression[1])
        return None

    @staticmethod
    def update_expression(
            expression_key: str,
            c: sqlite3.Cursor,
            expression_uri: t.Optional[str] = None,
            ):
        """
        :param expression_key: Key of the expression to update.
        :param expression_uri: The new expression URI.
        :param c: Database cursor.
        """
        update_query = '''
        UPDATE expression
        SET uri_expression = COALESCE(?, uri_expression)
        WHERE id = ?;
        '''
        c.execute(update_query, (expression_uri, expression_key))
        return

    @staticmethod
    def delete_expression(expression_key: str, c: sqlite3.Cursor):
        """
        :param expression_key: Key of the expression to delete.
        :param c: Database cursor.
        """
        delete_query = '''
        DELETE FROM expression
        WHERE id = ?;
        '''
        c.execute(delete_query, (expression_key,))
        return

class RecordRecordTable:

    @staticmethod
    def create_record_record_table(c: sqlite3.Cursor):
        create_table_query = '''
        CREATE TABLE record_links (
            record_key1 TEXT NOT NULL,
            record_key2 TEXT NOT NULL,
            PRIMARY KEY (record_key1, record_key2),
            FOREIGN KEY (record_key1) REFERENCES record (id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (record_key2) REFERENCES record (id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        '''
        c.execute(create_table_query)
        return
    
    @staticmethod
    def insert_link(
            record_key1: str,
            record_key2: str,
            c: sqlite3.Cursor,
            ):
        """
        :param record_key1: Key of record 1.
        :param record_key2: Key of record 2.
        :param c: Database cursor.
        """
        insert_query = 'INSERT INTO record_links (record_key1, record_key2) VALUES (?, ?);'
        c.execute(insert_query, (record_key1, record_key2,))
        return
    
    @staticmethod
    def fetch_linked_records(
            record_key: str,
            c: sqlite3.Cursor,
            ) -> t.Set[Record]:
        """
        :param record_key: Key of the record to get linked records off.
        :param c: Database cursor.

        :return: Set of records linked to the given record.
        """
        fetch_query = '''
        SELECT record_key1, record_key2
        FROM record_links
        WHERE record_key1 = ? OR record_key2 = ?;
        '''
        c.execute(fetch_query, (record_key, record_key,))
        results = c.fetchall()
        linked_records = set()
        for record1, record2 in results:
            if record1 != record_key:
                record = RecordTable.fetch_record(record_key=record1, c=c)
                if record:
                    linked_records.add(record)
            else:
                assert record2 != record_key
                record = RecordTable.fetch_record(record_key=record2, c=c)
                if record:
                    linked_records.add(record)
        return linked_records

    @staticmethod
    def delete_link(
            record_key1: str,
            record_key2: str,
            c: sqlite3.Cursor,
            ):
        """
        :param record_key1: Key of record 1.
        :param record_key2: Key of record 2.
        :param c: Database cursor.
        """
        delete_query = '''
        DELETE FROM record_links
        WHERE (record_key1 = ? AND record_key2 = ?)
           OR (record_key1 = ? AND record_key2 = ?);
        '''
        c.execute(delete_query, (record_key1, record_key2, record_key2, record_key1,))
        return

class RecordTagTable:

    @staticmethod
    def create_record_tag_table(c: sqlite3.Cursor):
        # Create the join table for record and tag
        create_table_query = '''
        CREATE TABLE record_tag (
            record_key TEXT,
            tag_name TEXT,
            FOREIGN KEY (record_key) REFERENCES record(id) ON DELETE CASCADE ON UPDATE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES tag(name) ON DELETE CASCADE ON UPDATE CASCADE,
            PRIMARY KEY (record_key, tag_name)
        );
        '''
        c.execute(create_table_query)
        return
    
    @staticmethod
    def insert_link(
        record_key: str,
        tag_name: str,
        c: sqlite3.Cursor):
        """
        :param record_key: Key of the record to link.
        :param tag_name: Name of the tag to link.
        :param c: Database cursor.
        """
        insert_link_query = '''
        INSERT INTO record_tag (record_key, tag_name) VALUES (?, ?);
        '''
        c.execute(insert_link_query, (record_key, tag_name))
        return

    @staticmethod
    def get_linked_tags(
        record_key: str,
        c: sqlite3.Cursor) -> t.Set[Tag]:
        """
        :param record_key: Key of the record to get the linked tags of.
        :param c: Database cursor.

        :return: Set of linked tags.
        """
        get_tags_query = '''
        SELECT tag_name
        FROM record_tag
        WHERE record_key = ?;
        '''
        c.execute(get_tags_query, (record_key,))
        return set([Tag(name=row[0]) for row in c.fetchall()])

    @staticmethod
    def get_linked_records(
        tag_name: str,
        c: sqlite3.Cursor) -> t.Set[Record]:
        """
        :param tag_name: Name of the tag to get the linked records of.
        :param c: Database cursor.

        :return: Set of records linked to the given Tag name.
        """
        get_records_query = '''
        SELECT record_key
        FROM record_tag
        WHERE tag_name = ?;
        '''
        c.execute(get_records_query, (tag_name,))
        record_keys = [row[0] for row in c.fetchall()]
        records = set()
        for record_key in record_keys:
            record = RecordTable.fetch_record(record_key=record_key, c=c)
            assert record is not None
            records.add(record)
        return records

    @staticmethod
    def delete_link(
        record_key: str,
        tag_name: str,
        c: sqlite3.Cursor):
        """
        :param record_key: Record key part of the link to remove.
        :param tag_name: Tag name part of the link to remove.
        :param c: Database cursor.
        """
        delete_link_query = '''
        DELETE FROM record_tag
        WHERE record_key = ? AND tag_name = ?;
        '''
        c.execute(delete_link_query, (record_key, tag_name))
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