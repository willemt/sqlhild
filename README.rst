Command line
============
You can run queries from the command line.

.. code-block:: bash
   :class: ignore

   sqlhild 'select * from `sqlhild.example.OneToTen`'

You can load modules.

.. code-block:: bash
   :class: ignore

   sqlhild -m botoquery/__init__.py "select EnvironmentName from EBEnvironment"

If you don't have command line options you can just pass the whole SQL query as if the command was "echo". Watch out for shell globbing!

.. code-block:: bash
   :class: ignore

   sqlhild select '*' from `sqlhild.example.OneToTen`

You can put the module path as the parent of your table

.. code-block:: bash
   :class: ignore

   sqlhild select '*' from `botoquery/ecs.py.Clusters`

Postgres mode
=============
sqlhild in server mode runs a Postgres facade. You can use your favourite Postgres client to play around with sqlhild tables.

.. code-block:: bash
   :class: ignore

   sqlhild --server 0.0.0.0:10000 --modules sqlhild.example
