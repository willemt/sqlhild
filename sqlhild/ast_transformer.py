from ast import (
    Call,
    Load,
    Name,
    NodeTransformer,
)


class FindAndReplaceNames(NodeTransformer):
    """
    Searches this AST and replaces names.
    """
    def __init__(self, replacements):
        # TODO: raise exception if replacement not done
        self.replacements = replacements
        super().__init__()

    def visit_Name(self, node):
        try:
            replacement = self.replacements[node.id]
        except (KeyError, AttributeError):
            return node
        else:
            return Name(id=replacement.name, ctx=Load())
