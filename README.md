Overview
========

[![Build Status](https://api.travis-ci.org/flussonic/sqlapi.png)](https://travis-ci.org/flussonic/sqlapi)


This library helps you to create an erlang application into a SQL server.

You will be able to access internal of running erlang server as if it is a SQL database with tables.




Why not just HTTP
-----------------

This is a replacement to usual HTTP JSON/XML API and can be more convenient because SQL gives you
sorting, paging, filtering out of the box and HTTP API usually has all this designed ad-hoc.


This piece of code handles `ORDER BY`, `LIMIT`, `OFFSET`, etc, out of the box.


Main idea was to make it accessible from modern ORMs like Ruby on Rails, Flask and so on.

It happened to be very convenient, because ORM engines are usually much better developed and supported
than ad-hoc implementation of http api library that has to be rewritten for each service and protocol.




How it differs from myproto
---------------------------


Our `sqlapi` projects relies on parser of mysql protocol. Not a best solution, very sorry, once we will add postgresql 
support. `https://github.com/altenwald/myproto` was selected as an implementation of wire protocol that
can decode and encode packets and parse SQL. We had to modify it a lot and remove some pieces of code,
so now there is not too much from `myproto` (see LEGAL file). Mostly we had to add lot of 
code that implements behaviour of MySQL server.

`myproto` only parses queries, but we need to handle them and reply. While adding support for more and more ORM
we have met different amazing ways to read the same data. For example, each ORM has it's own unique way to get
list of tables and columns. It is not a problem of ORM's, it is a problem of MySQL that allows so many undocumented
ways to get data that anyone requires. This is why we want once to migrate to postgresql protocol and forget about mysql.

Our code offers a lot of default implementations of queries like `SHOW FIELDS` or `SELECT @@global.max_allowed_packet`
All these queries are useless outside of MySQL server, but many ORM will not work if you do not reply properly on these
requests.


It is important to understand that while implementing this `sqlapi` we had to make a compromise between big list of
possible features available in SQL and simplicity of usage. Simplicity was selected.
For example you cannot handle `CREATE DATABASE` or `SET @key='value'`



How to use it
-------------

You need to write your own handler that will handle business logic.

We have made an example called `sql_csv_api` just like a CSV storage in MySQL to illustrate what can be done.

Here is the list of functions required to be implemented:


* `authorize(Username,HashedPassword,ClientSalt,Args) -> {ok, State}`  `Args` here are passed from initialization, will describe below. Simplest implementation is: `<<"user">> == Username andalso sqlapi:password_hash(<<"password">>, ClientSalt) == HashedPassword`
* `connect_db(Database::binary(), State) -> {ok, State1} | {error, Code, Description}`.  Here and further you can reply with error and error code. Please, try to use mysql error codes. This function will connect engine to database, remember this database in your state.
* `databases(State) -> [DBName::binary()]` returns list of available databases, just binary strings.
* `database(State) -> undefined | DBName` return currently connected database.
* `tables(State) -> [TableName::binary()]` returns list of tables in connected database. We will not allow to call it before user connects to database, you can rely on it. This is also checked for all other functions that require connection to some database. 
* `columns(Table::binary(),State) -> [{ColumnName::atom(),ColumnType::type()}]`  Type may be: `string`, `boolean` or `integer`
* `terminate(Reason,State)` maybe will be called on session closing
* `select(Table::binary(),Conditions,State) -> [#{} = Row]`  here you can take a look at `Conditions`. This is a complicated structure, described below
* `insert(Table::binary(), [#{Key::binary() => Value}], State) -> {ok,#{status =>ok, affected_rows=>1}} | {error,Code,Desc}` will be called on inserting rows
* `update(Table::binary(), #{Key::binary() => Value}, Conditions, State) -> {ok,#{status =>ok, affected_rows=>1}} | {error,Code,Desc}` will be called on updating. `Conditions` just as in `select` and only one update row is specified.
* `delete(Table::binary(), Conditions, State) -> {ok,#{status =>ok, affected_rows=>1}} | {error,Code,Desc}` will be called on deleting from table. `Conditions` are used from `select`
* `fncall(Name::binary(), [Param], State)` is called on SQL function call







