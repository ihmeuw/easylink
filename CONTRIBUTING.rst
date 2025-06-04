============
Contributing
============

To run or develop easylink tests or documentation, you can install easylink with the ``[dev]`` extra::

    $ git clone git@github.com:ihmeuw/easylink.git # or git clone https://github.com/ihmeuw/easylink.git
    $ cd easylink
    $ pip install -e .[dev]

The ``-e`` flag installs EasyLink as `editable`, which allows you to make changes to the code and docstrings
and immediately have them reflected in your installed version. If you didn't install editably, you would 
have to reinstall to see your changes.

.. todo::
    Show how to run the tests and build the documentation