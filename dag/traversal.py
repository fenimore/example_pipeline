import pkgutil
import sys
import attr
import luigi
from luigi.task import flatten
from collections import deque

from dag.pipeline import HoroscopeReportTask


@attr.s
class Node(object):
    """
    this is a Task
    """
    name: str = attr.ib()
    output: str = attr.ib() # output path
    params = attr.ib()
    inputs = attr.ib() # childrens' outputs
    requires = attr.ib() # children class names
    # len_children = attr.ib()
    _children = attr.ib() # class instances

    @classmethod
    def get_node(cls, task):
        return cls(
            name=task.__class__.__name__,
            output=task.output().path,
            params=sorted(task.to_str_params(only_significant=True).keys()),
            inputs=sorted([i.path for i in flatten(task.input())]),
            requires=sorted([d.__class__.__name__ for d in flatten(task.requires())]),
            children=flatten(task.requires()),
        )


def bf_traversal(task):
    "breadth first traversal"
    node = Node.get_node(task)
    result = [node]
    children = deque(node._children)
    while children:
        child = Node.get_node(children.popleft())
        children.extend(child._children)
        result.append(child)
        if not children:
            break
    return result


def df_traversal(task, accum):
    "depth first traversal"
    node = Node.get_node(task)
    accum.append(node)
    if not node._children:
        return accum

    for child in node._children:
        culm = df_traversal(child, accum)
    return culm


if __name__ == '__main__':
    from datetime import datetime
    dag = bf_traversal(
        HoroscopeReportTask(datetime.now())
    )
    print("BF Traversal")
    print(len(dag))
    padding = ""
    for x in dag:
        print(padding + x.name + " - " + x.output)
        if not padding:
            padding += " "

    dag = df_traversal(
        HoroscopeReportTask(datetime.now()),
        list()
    )
    print("")
    print(" DF Traversal")
    print(" " + str(len(dag)))
    padding = ""
    for x in dag:
        print(padding + x.name + " - " + x.output)
        if not padding:
            padding += " "
