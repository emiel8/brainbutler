from abc import ABC, abstractmethod
import typing as t

from ..components.record import Record
from ..components.tag import Tag
from ..components.node import Node
from ..components.expression import Expression

class DatabaseAdapter(ABC):

    @abstractmethod
    def insert_record(self, text_uri: str, image_uri: str, sound_uri: str, reference: str) -> str:
        """Insert a new record into the database.

        :param text_uri: URI of the text file of the record.
        :param image_uri: URI of the image file of the record.
        :param sound_uri: URI of the sound file of the record.
        :param reference: Reference of the record.

        :return: Key of the record.
        """
        
    @abstractmethod
    def insert_tag(self, tag_name: str) -> str:
        """Insert a new tag into the database.

        :param tag_name: Name of the tag to insert.

        :return: Name of the tag.
        """

    @abstractmethod
    def insert_node(self, node_uri: str) -> str:
        """Insert a new node into the database.

        :param node_uri: URI of the node to insert

        :return: Key of the node.
        """

    @abstractmethod
    def insert_expression(self, expression_uri: str) -> str:
        """Insert a new expression into the database.

        :param expression_uri: URI of the expression to insert.

        :return: Key of the expression.
        """

    @abstractmethod
    def link_record_and_record(self, record_key1: str, record_key2: str) -> None:
        """Link the 2 given records.

        :param record_key1: The key of the record to link.
        :param record_key2: The key of the other record to link.
        """

    @abstractmethod
    def link_record_and_tag(self, record_key: str, tag_key: str) -> None:
        """Link the given record and tag.

        :param record_key: The key of the record to link.
        :param tag_key: The key of the tag to link.
        """
    
    @abstractmethod
    def link_record_and_node(self, record_key: str, node_key: str) -> None:
        """Link the given record and node.

        :param record_key: The key of the record to link.
        :param node_key: The key of the node to link.
        """
    
    @abstractmethod
    def link_node_and_expression(self, node_key: str, expression_key: str) -> None:
        """Link the given node and expression.

        :param node_key: The key of the node to link.
        :param expression_key: The key of the expression to link.
        """
    
    @abstractmethod
    def link_node_and_tag(self, node_key: str, tag_key: str) -> None:
        """Link the given node and tag.

        :param node_key: The key of the node to link.
        :param tag_key: The key of the tag to link.
        """
    
    @abstractmethod
    def link_expression_and_tag(self, expression_key: str, tag_key: str) -> None:
        """Link the given expression and tag.

        :param expression_key: The key of the expression to link.
        :param tag_key: The key of the tag to link.
        """

    @abstractmethod
    def unlink_record_and_record(self, record_key1: str, record_key2: str) -> None:
        """Unlink the 2 given records.

        :param record_key1: The key of the record to unlink.
        :param record_key2: The key of the other record to unlink.
        """

    @abstractmethod
    def unlink_record_and_tag(self, record_key: str, tag_key: str) -> None:
        """Unlink the given record and tag.

        :param record_key: The key of the record to unlink.
        :param tag_key: The key of the tag to unlink.
        """
    
    @abstractmethod
    def unlink_record_and_node(self, record_key: str, node_key: str) -> None:
        """Unlink the given record and node.

        :param record_key: The key of the record to unlink.
        :param node_key: The key of the node to unlink.
        """
    
    @abstractmethod
    def unlink_node_and_expression(self, node_key: str, expression_key: str) -> None:
        """Unlink the given node and expression.

        :param node_key: The key of the node to unlink.
        :param expression_key: The key of the expression to unlink.
        """
    
    @abstractmethod
    def unlink_node_and_tag(self, node_key: str, tag_key: str) -> None:
        """Unlink the given node and tag.

        :param node_key: The key of the node to unlink.
        :param tag_key: The key of the tag to unlink.
        """
    
    @abstractmethod
    def unlink_expression_and_tag(self, expression_key: str, tag_key: str) -> None:
        """Unlink the given expression and tag.

        :param expression_key: The key of the expression to unlink.
        :param tag_key: The key of the tag to unlink.
        """

    @abstractmethod
    def fetch_record(self, key: str) -> Record:
        """ Retrieve a record from the database

        :param key: The unique key of the record to retrieve.

        :return: The requested record.
        """

    @abstractmethod
    def fetch_tag(self, key: str) -> Tag:
        """ Retrieve a tag from the database

        :param key: The unique key of the tag to retrieve.

        :return: The requested tag.
        """
    
    @abstractmethod
    def fetch_node(self, key: str) -> Node:
        """ Retrieve a node from the database

        :param key: The unique key of the node to retrieve.

        :return: The requested node.
        """

    @abstractmethod
    def fetch_expression(self, key: str) -> Expression:
        """ Retrieve an expressoin from the database

        :param key: The unique key of the expression to retrieve.

        :return: The requested Expression.
        """

    @abstractmethod
    def fetch_tags_of_record(self, record_key: str) -> t.Set[Tag]:
        """ Retrieve tags linked to the given record_key

        :param record_key: The record key for which to get the linked tags.

        :return: Set of the tags linked to the given record.
        """

    @abstractmethod
    def fetch_records_of_tag(self, tag_key: str) -> t.Set[Record]:
        """ Retrieve records linked to the given tag_key

        :param tag_key: The tag key for which to get the linked records.

        :return: Set of the records linked to the given tag. 
        """

    @abstractmethod
    def fetch_tags_of_node(self, node_key: str) -> t.Set[Tag]:
        """ Retrieve tags linked to the given node_key

        :param node_key: The node key for which to get the linked tags.

        :return: Set of the tags linked to the given node.
        """

    @abstractmethod
    def fetch_nodes_of_tag(self, tag_key: str) -> t.Set[Node]:
        """ Retrieve nodes linked to the given tag_key

        :param tag_key: The tag key for which to get the linked nodes.

        :return: Set of the nodes linked to the given tag.
        """

    @abstractmethod
    def fetch_tags_of_expression(self, expression_key: str) -> t.Set[Tag]:
        """ Retrieve tags linked to the given expression_key

        :param expression_key: The expression key for which to get the linked tags.

        :return: Set of the tags linked to the given expression.
        """

    @abstractmethod
    def fetch_expressions_of_tag(self, tag_key: str) -> t.Set[Expression]:
        """ Retrieve expressions linked to the given tag_key

        :param tag_key: The tag key for which to get the linked expressions.

        :return: Set of expressions linked to the given tag.
        """
    
    @abstractmethod
    def fetch_linked_records_of_record(self, record_key: str) -> t.Set[Record]:
        """ Retrieve records linked to the given record_key

        :param record_key: The record key for which to get the linked records.

        :return: Set of the records linked to the given record.
        """

    @abstractmethod
    def fetch_nodes_of_record(self, record_key: str) -> t.Set[Node]:
        """ Retrieve nodes that contain the given record.

        :param record_key: The record key for which to get the nodes that contain it.
        """
    
    @abstractmethod
    def fetch_records_of_node(self, node_key: str) -> t.Set[Record]:
        """ Retrieve all records in the given node.

        :param node_key: The node key for which to get the records that are contained in it.

        :return: The records contained in the given node.
        """
    
    @abstractmethod
    def fetch_nodes_of_expression(self, expression_key: str) -> t.Set[Node]:
        """ Retrieve all nodes contained in the given expression.

        :param expression_key: The expression key for which to get the nodes it contains.

        :return: The nodes contained in the given expression.
        """
    
    @abstractmethod
    def fetch_expressions_of_node(self, node_key: str) -> t.Set[Expression]:
        """ Retrieve all expressions that contain the given node.

        :param node_key: The node key for which to get the expressions that contain it.

        :return: The expressions that contain the given node.
        """
    
    def fetch_records_of_expression(self, expression_key: str) -> t.Set[Record]:
        """ Retrieve all records contained in the given expression.

        :param expression_key: The expression key for which to get the records it contains.

        :return: The records that are used in the given expression.
        """
        records = set()
        for node_key in self.fetch_nodes_of_expression(expression_key=expression_key):
            records.add(self.fetch_records_of_node(node_key=node_key))
        return records
    
    def fetch_expressions_of_record(self, record_key: str) -> t.Set[Expression]:
        """ Retrieve all expressions that contain the given record.

        :param record_key: The record key for which to get all expressions that contain it.
        """
        expressions = set()
        for node_key in self.fetch_nodes_of_record(record_key=record_key):
            expressions.add(self.fetch_expressions_of_node(node_key=node_key))
        return expressions

    @abstractmethod
    def delete_record(self, record_key: str) -> None:
        """Delete record from the database.

        :param record_key: Key of the record to delete.
        """
        
    @abstractmethod
    def delete_tag(self, tag_key: str) -> None:
        """Delete tag from the database.

        :param tag_key: Key of the tag to delete.
        """

    @abstractmethod
    def delete_node(self, node_key: str) -> None:
        """Delete node from the database.

        :param node_key: Key of the node to delete
        """

    @abstractmethod
    def delete_expression(self, expression_key: str) -> None:
        """Delete expression from the database.

        :param expression_key: Key of the expression to delete.
        """