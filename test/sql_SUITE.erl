-module(sql_SUITE).
% derived from https://github.com/altenwald/myproto under EPL:
% https://github.com/altenwald/myproto/commit/d89e6cad46fa966e6149aee3ad6f0d711e6182f5
-author('Manuel Rubio <manuel@altenwald.com>').
-author('Max Lapshin <max@maxidoors.ru>').


-include("../src/myproto.hrl").
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).


all() ->
  [{group,parse},
  {group,queries}].


groups() ->
  [{parse, [parallel], [
    delete_simple,
    delete_where,
    insert_simple,
    insert_keys,
    insert_set,
    insert_multi,
    describe,
    show,
    show_like,
    transaction,
    set,
    select_all,
    select_strings,
    select_simple,
    select_simple_multiparams,
    select_simple_subquery,
    select_from,
    select_from_subquery,
    select_where,
    select_function,
    select_groupby,
    select_orderby,
    select_limit,
    select_arithmetic,
    select_variable,
    select_in,

    server_select_simple,
    server_reject_password,
    server_version,
    server_very_long_query,
    update_simple,
    update_multiparams,
    update_where,
    backslash
  ]},
  {queries, [parallel], [
    long_query_2,
    select_database_before_connect,
    login_as_sequelpro,
    hanami_rom_support,
    rails_support,
    no_db_selected,
    describe_sql,
    order_by,
    work_with_ram_table
  ]}].


init_per_suite(Config) ->
  application:ensure_started(sqlapi),
  sqlapi:load_config(#{port => 4406, listener_name => test_sql, handler => test_handler, trivial => true}),
  Config.

end_per_suite(Config) -> 
  sqlapi:load_config(#{handler => test_handler, listener_name => test_sql, trivial => true}),
  Config.




select_database_before_connect(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db"),
  nanomysql:command(ping, <<>>, C),
  [#{'database()' := undefined}] = nanomysql:select("SELECT DATABASE()", C),
  ok.

login_as_sequelpro(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db"),
  nanomysql:command(ping, <<>>, C),
  {ok, _} = nanomysql:execute("SHOW VARIABLES", C),
  [#{'@@global.max_allowed_packet' := 4194304}] = nanomysql:select("SELECT @@global.max_allowed_packet", C),
  {ok, _} = nanomysql:execute("USE `test_db`", C),
  [#{'database()' := <<"test_db">>}] = nanomysql:select("SELECT DATABASE()", C),

  [#{'@@tx_isolation' := <<"REPEATABLE-READ">>}] = nanomysql:select("SELECT @@tx_isolation", C),
  ok.




no_db_selected(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406"),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("DESCRIBE `test`",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("SHOW TABLES",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("SHOW INDEX FROM `test`",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("SHOW FIELDS FROM `test`",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("SELECT * FROM `test`",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("DELETE FROM `test`",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("UPDATE `test` SET a=5",C),
  {error, {1046, <<"No database selected">>}} = nanomysql:execute("INSERT INTO `test` VALUES (5)",C),
  ok.


hanami_rom_support(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db?login=init_db"),
  {ok, _} = nanomysql:execute("SET SQL_AUTO_IS_NULL=0",C),
  {ok, _} = nanomysql:execute("DESCRIBE `test`",C),
  {ok, _} = nanomysql:execute("SHOW INDEX FROM `test`",C),
  {ok, _} = nanomysql:execute("SELECT `CONSTRAINT_NAME` AS `name`, `COLUMN_NAME` AS `column`, "
    "`REFERENCED_TABLE_NAME` AS `table`, `REFERENCED_COLUMN_NAME` AS `key` FROM "
    "`INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` WHERE ((`TABLE_NAME` = 'test') "
    "AND (`TABLE_SCHEMA` = DATABASE()) AND (`CONSTRAINT_NAME` != 'PRIMARY') AND "
    "(`REFERENCED_TABLE_NAME` IS NOT NULL))",C),
  {ok, _} = nanomysql:execute("SELECT NULL AS `nil` FROM `test` LIMIT 1",C),
  ok.


rails_support(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db?login=init_db"),
  Rows1 = nanomysql:select("SELECT table_name FROM information_schema.tables WHERE table_schema = database()",C),
  Rows2 = lists:sort(Rows1),

  [#{'Tables_in_test_db' := <<"test">>}|_] = Rows2,
  ok.


describe_sql(_) ->
  {ok, C} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db?login=init_db"),
  Rows = nanomysql:select("DESCRIBE `test`", C),
  #{'Field' := <<"id">>, 'Type' := <<"varchar(255)">>} = hd(Rows),
  ok.



work_with_ram_table(_) ->
  {ok, Conn} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db?login=init_db"),
  ok = sqlapi:create_temporary_table(user_sessions, [
    {user_id, integer}, {session_id, string}, {access_time, integer}
  ]),
  {ok ,_} = nanomysql:execute("insert into user_sessions (user_id,session_id,access_time) values (1,'u1',1540123456)", Conn),
  [#{user_id := 1, session_id := <<"u1">>, access_time := 1540123456}] = nanomysql:select("select * from user_sessions", Conn),

  nanomysql:execute("update user_sessions set access_time = 14123123123 where user_id=1", Conn),
  [#{user_id := 1, access_time := 14123123123}] = nanomysql:select("select * from user_sessions", Conn),

  nanomysql:execute("delete from user_sessions where user_id=1", Conn),
  [] = nanomysql:select("select * from user_sessions", Conn),
  ok.



order_by(_) ->
  {ok, Conn} = nanomysql:connect("mysql://user:pass@127.0.0.1:4406/test_db?login=init_db"),
  ok = sqlapi:create_temporary_table(ordered_streams, [
    {name,string},{title,string},{extra,string},{hls_off,boolean}
  ]),


  {ok ,_} = nanomysql:execute("insert into ordered_streams (name,title,extra,hls_off) values ('ob2','ob', '', 1)", Conn),
  {ok ,_} = nanomysql:execute("insert into ordered_streams (name,title,extra,hls_off) values ('ob1','ob', null, 0)", Conn),
  {ok ,_} = nanomysql:execute("insert into ordered_streams (name,title,extra,hls_off) values ('ob3','ob', 'aa', 1)", Conn),
  {ok ,_} = nanomysql:execute("insert into ordered_streams (name,title,extra,hls_off) values ('ob4','ob', null, 0)", Conn),

  % Эти две строчки ниже я закомментировал потому что они по определению лишены смысла
  % Без сортировки может быть произвольный результат
  % Unsorted = nanomysql:select("select name,title from ordered_streams where title='ob'", Conn),
  % Unsorted = nanomysql:select("select name,title from ordered_streams where title='ob' order by title", Conn),
  [#{name := <<"ob4">>}, #{name := <<"ob3">>}, #{name := <<"ob2">>}, #{name := <<"ob1">>}] = nanomysql:select("select * from ordered_streams where title='ob' order by name desc", Conn),
  [#{name := <<"ob1">>}, #{name := <<"ob4">>}, #{name := <<"ob2">>}, #{name := <<"ob3">>}] = nanomysql:select("select * from ordered_streams where title='ob' order by hls_off asc, name asc", Conn),
  % mysql null first when asc
  [#{name := <<"ob1">>}, #{name := <<"ob4">>}, #{name := <<"ob2">>}, #{name := <<"ob3">>}] = nanomysql:select("select * from ordered_streams where title='ob' order by extra asc, name asc", Conn),
  % mysql null last when desc
  [#{name := <<"ob3">>}, #{name := <<"ob2">>}, #{name := <<"ob1">>}, #{name := <<"ob4">>}] = nanomysql:select("select * from ordered_streams where title='ob' order by extra desc, name asc", Conn),
  [] = nanomysql:select("select title,name from ordered_streams where title='ob!' order by name", Conn),
  try nanomysql:select("select * from ordered_streams where title='ob' order by name asc, nnaammee desc", Conn) of
    _ -> ct:fail("no error on unknown column")
  catch
    error:{1036, <<"unknown column in order clause">>} -> ok
  end.





transaction(_) ->
  'begin' = sql92:parse("begin"),
  'commit' = sql92:parse("commit"),
  'rollback' = sql92:parse("rollback"),
  ok.


describe(_) ->
  #describe{table = #table{name = <<"streams">>}} = sql92:parse("DESCRIBE `streams`").

delete_simple(_) ->
  #delete{table=#table{name = <<"mitabla">>, alias = <<"mitabla">>}} = sql92:parse("delete from mitabla").


delete_where(_) ->
  #delete{
        table=#table{name = <<"mitabla">>, alias = <<"mitabla">>},
        conditions=#condition{
            nexo=eq,
            op1=#key{name = <<"dato">>, alias = <<"dato">>},
            op2=#value{value = <<"this ain't a love song">>}
        }
  } = sql92:parse("delete from mitabla where dato='this ain''t a love song'").



insert_simple(_) ->
  #insert{table = #table{name = <<"mitabla">>, alias = <<"mitabla">>}, values=[
    [#value{value=1}, #value{value=2}, #value{value=3}]
  ]} = sql92:parse("insert into mitabla values (1,2,3)").


insert_keys(_) ->
  #insert{table = #table{name = <<"mitabla">>,
                 alias = <<"mitabla">>},
  values = [[#set{key = <<"id">>,
                 value = #value{value = 1}},
            #set{key = <<"author">>,
                 value = #value{value = <<"bonjovi">>}},
            #set{key = <<"song">>,
                 value = #value{value = <<"these days">>}}]]} =
  sql92:parse("insert into mitabla(id,author,song) values(1,'bonjovi', 'these days')").


insert_set(_) ->
  A = sql92:parse("insert into mitabla(id,author,song) values(1,'bonjovi', 'these days')"),
  B = sql92:parse("insert into mitabla set id=1, author='bonjovi', song='these days'"),
  A = B.


insert_multi(_) ->
  A = sql92:parse("insert into mitabla(id) values(1),(2)"),
  #insert{table = #table{name = <<"mitabla">>, alias = <<"mitabla">>}, values=[
    [#set{key = <<"id">>, value = #value{value=1}}],
    [#set{key = <<"id">>, value = #value{value=2}}]
  ]} = A.

show(_) ->
  #show{type=databases} = sql92:parse("SHOW databases"),
  #show{type=variables} = sql92:parse("SHOW variables"),
  #show{type=tables, full = true} = sql92:parse("SHOW FULL tables"),
  #show{type=tables, full = false} = sql92:parse("SHOW tables"),
  #show{type=fields,full=true,from= <<"streams">>} = sql92:parse("SHOW FULL FIELDS FROM `streams`"),
  #show{type=fields,full=false,from= <<"streams">>} = sql92:parse("SHOW FIELDS FROM `streams`"),
  #show{type=index,full=false,from= <<"streams">>} = sql92:parse("SHOW INDEX FROM `streams`"),
  #show{type=create_table,from= <<"streams">>} = sql92:parse("SHOW CREATE TABLE `streams`"),
  #show{type=tables,full=false,conditions= {like,<<"streams">>}} = sql92:parse("SHOW TABLES LIKE 'streams'"),
  #show{type=variables, conditions=#condition{
    nexo = eq,
    op1 = #key{name = <<"Variable_name">>},
    op2 = #value{value = <<"character_set_client">>}
  }} = sql92:parse("SHOW VARIABLES WHERE Variable_name = 'character_set_client'"),

  #show{type=collation, conditions=#condition{
    nexo = eq,
    op1 = #key{name= <<"Charset">>},
    op2 = #value{value = <<"utf8">>}
  }} = sql92:parse("show collation where Charset = 'utf8'"),
  ok.


show_like(_) ->
  #show{type=variables, conditions = {like, <<"sql_mode">>}} = sql92:parse("SHOW VARIABLES LIKE 'sql_mode'"),
  ok.



set(_) ->
  #system_set{query=[{#variable{name = <<"a">>, scope = session},#value{value=0}}]} = sql92:parse("SET a=0"),
  #system_set{query=[{#variable{name = <<"NAMES">>},#value{value= <<"utf8">>}}]} = sql92:parse("SET NAMES 'utf8'"),
  #system_set{query=[{#variable{name = <<"NAMES">>},#value{value= <<"utf8">>}}]} = sql92:parse("SET NAMES utf8"),

  #system_set{query=[
      {#variable{name = <<"SQL_AUTO_IS_NULL">>, scope = session}, #value{value=0}},
      {#variable{name = <<"NAMES">>},#value{value= <<"utf8">>}},
      {#variable{name = <<"wait_timeout">>, scope = local}, #value{value=2147483}}
  ]} = sql92:parse("SET SQL_AUTO_IS_NULL=0, NAMES 'utf8', @@wait_timeout = 2147483"),


  #system_set{query = [
    {#variable{name = <<"SESSION.sql_mode">>, scope = local}, #function{}},
    {#variable{name = <<"SESSION.sql_auto_is_null">>, scope=local},#value{value= 0}},
    {#variable{name = <<"SESSION.wait_timeout">>, scope = local}, #value{value=2147483}}
  ]} = sql92:parse("SET  @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'), "
    " @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483"),
  ok.


select_variable(_) ->
  #select{params = [#variable{name = <<"max_allowed_packet">>, scope = local}]} = 
    sql92:parse("SELECT @@max_allowed_packet"),
  #select{params = [#variable{name = <<"global.max_allowed_packet">>, scope = local}]} = 
    sql92:parse("SELECT @@global.max_allowed_packet"),

  #select{params = [#variable{name = <<"global.max_allowed_packet">>, scope = local}], limit = 1} = 
    sql92:parse("SELECT @@global.max_allowed_packet limit 1"),

  #select{tables = [#table{name= {<<"information_schema">>,<<"key_column_usage">>}}]} = 
    sql92:parse("SELECT `CONSTRAINT_NAME` AS `name`, `COLUMN_NAME` AS `column`, `REFERENCED_TABLE_NAME` AS `table`, "
      "`REFERENCED_COLUMN_NAME` AS `key` FROM `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` WHERE "
      "((`TABLE_NAME` = 'streams') AND (`TABLE_SCHEMA` = DATABASE()) AND (`CONSTRAINT_NAME` != 'PRIMARY') "
      "AND (`REFERENCED_TABLE_NAME` IS NOT NULL))"),

  ok.


select_in(_) ->
  #select{params=[#all{}],
    conditions=#condition{nexo=in,
      op1 = #key{name= <<"n">>},
      op2 = #subquery{subquery = [<<"a">>,<<"b">>]}
    }
  } = sql92:parse(<<"SELECT * from b where n in ('a','b') order by a">>),
  #select{params=[#all{}],
    conditions=#condition{nexo=not_in,
      op1 = #key{name= <<"n">>},
      op2 = #subquery{subquery = [<<"a">>,<<"b">>]}
      }
  } = sql92:parse(<<"SELECT * from b where n not in ('a','b') order by a">>),
  ok.


select_all(_) ->
  #select{params=[#all{}]} = sql92:parse("select *"),
  #select{params=[#all{}]} = sql92:parse("SELECT *"),
  #select{params=[#all{}]} = sql92:parse(" Select    *   "),
  ok.

select_strings(_) ->
  #select{params = [#value{value = <<"hola'mundo">>}]} = sql92:parse("select 'hola''mundo'").


select_simple(_) ->
    ?assertEqual(sql92:parse("select 'hi' as message"),
        #select{params=[#value{name = <<"message">>,value = <<"hi">>}]}
    ),
    ?assertEqual(sql92:parse("select 'hi'"),
        #select{params=[#value{value = <<"hi">>}]}
    ),
    ?assertEqual(sql92:parse("select hi"),
        #select{params=[#key{alias = <<"hi">>,name = <<"hi">>}]}
    ),
    ?assertEqual(sql92:parse("select hi as hello"),
        #select{params=[#key{alias = <<"hello">>,name = <<"hi">>}]}
    ),
    ?assertEqual(sql92:parse("select a.hi"),
        #select{params=[#key{alias = <<"hi">>,name = <<"hi">>,table = <<"a">>}]}
    ),
    ?assertEqual(sql92:parse("select aa.hi as hello"),
        #select{params=[#key{alias = <<"hello">>,name = <<"hi">>,table = <<"aa">>}]}
    ),
    ok.

select_simple_multiparams(_) ->
    ?assertEqual(sql92:parse("select 'hi' as message, 1 as id"),
        #select{params=[#value{name = <<"message">>,value = <<"hi">>},#value{name = <<"id">>,value=1}]}
    ),
    ?assertEqual(sql92:parse("select 'hi', 1"),
        #select{params=[#value{value = <<"hi">>},#value{value=1}]}
    ),
    ?assertEqual(sql92:parse("select hi, message"),
        #select{params=[#key{alias = <<"hi">>,name = <<"hi">>},
        #key{alias = <<"message">>,name = <<"message">>}]}
    ),
    ?assertEqual(sql92:parse("select hi as hello, message as msg"),
        #select{params=[#key{alias = <<"hello">>,name = <<"hi">>},
        #key{alias = <<"msg">>,name = <<"message">>}]}
    ),
    ?assertEqual(sql92:parse("select a.hi, a.message"),
        #select{params=[#key{alias = <<"hi">>,name = <<"hi">>,table = <<"a">>},
        #key{alias = <<"message">>,name = <<"message">>,table = <<"a">>}]}
    ),
    ?assertEqual(sql92:parse("select aa.hi as hello, aa.message as msg"),
        #select{params=[#key{alias = <<"hello">>,name = <<"hi">>,table = <<"aa">>},
        #key{alias = <<"msg">>,name = <<"message">>,table = <<"aa">>}]}
    ),
    ?assertEqual(sql92:parse("select a.*, b.*"),
        #select{params=[#all{table = <<"a">>}, #all{table = <<"b">>}]}
    ),
    ?assertEqual(sql92:parse("select *, a.*, b.*"),
        #select{params=[#all{}, #all{table = <<"a">>}, #all{table = <<"b">>}]}
    ),
    ok.

select_simple_subquery(_) ->
    ?assertEqual(sql92:parse("select (select *)"),
        #select{params=[#subquery{subquery=#select{params=[#all{}]}}]}
    ),
    ?assertEqual(sql92:parse("select (select uno) as uno, dos"),
        #select{params=[#subquery{name = <<"uno">>,
                   subquery=#select{params=[#key{alias = <<"uno">>,name = <<"uno">>}]}},
        #key{alias = <<"dos">>,name = <<"dos">>}]}
    ),
    ok.

select_from(_) ->
    ?assertEqual(sql92:parse("select * from data"),
        #select{params=[#all{}],tables=[#table{name = <<"data">>,alias = <<"data">>}]}
    ),
    ?assertEqual(sql92:parse("select uno, dos from data, data2"),
        #select{params = [#key{alias = <<"uno">>,name = <<"uno">>},
                  #key{alias = <<"dos">>,name = <<"dos">>}],
        tables = [#table{name = <<"data">>,alias = <<"data">>},
                  #table{name = <<"data2">>,alias = <<"data2">>}]}
    ),
    ?assertEqual(sql92:parse("select d.uno, d2.dos from data as d, data2 as d2"),
        #select{params = [#key{alias = <<"uno">>,name = <<"uno">>,
                       table = <<"d">>},
                  #key{alias = <<"dos">>,name = <<"dos">>,table = <<"d2">>}],
        tables = [#table{name = <<"data">>,alias = <<"d">>},
                  #table{name = <<"data2">>,alias = <<"d2">>}]}
    ),

    #select{params = [#all{}], tables =[#table{name= <<"streams">>}],
      order = [#order{key= <<"name">>, sort = asc}], limit =1} =
    sql92:parse("SELECT `streams`.* FROM `streams` ORDER BY `streams`.`name` ASC LIMIT 1"),

    #select{params = [#key{name = <<"table_name">>}], 
      tables = [#table{name = {<<"information_schema">>,<<"tables">>}}]} = 
      sql92:parse("SELECT table_name FROM information_schema.tables WHERE table_schema = database()"),
    ok.

select_from_subquery(_) ->
    ?assertEqual(sql92:parse("select * from (select 1 as uno,2 as dos)"),
        #select{
    params = [#all{}],
    tables = 
        [#subquery{
             subquery = 
                 #select{
                     params = 
                         [#value{name = <<"uno">>,value = 1},
                          #value{name = <<"dos">>,value = 2}]}}]}
    ),
    ?assertEqual(sql92:parse("select (select 1) as id, t.uno from (select 2) as t"),
        #select{
    params = 
        [#subquery{
             name = <<"id">>,
             subquery = 
                 #select{
                     params = [#value{name = undefined,value = 1}]}},
         #key{alias = <<"uno">>,name = <<"uno">>,table = <<"t">>}],
    tables = 
        [#subquery{
             name = <<"t">>,
             subquery = 
                 #select{
                     params = [#value{value = 2}]}}]}
    ),
    ?assertEqual(sql92:parse("select * from clientes where id in ( 1, 2, 3 )"),
        #select{params = [#all{}],
        tables = [#table{name = <<"clientes">>,
                         alias = <<"clientes">>}],
        conditions = #condition{nexo = in,
                                op1 = #key{alias = <<"id">>,name = <<"id">>},
                                op2 = #subquery{subquery = [1,2,3]}}}
    ),
    ok.

select_where(_) ->
    ?assertEqual(sql92:parse("select * from tabla where uno=1"),
        #select{params = [#all{}],
        tables = [#table{name = <<"tabla">>,alias = <<"tabla">>}],
        conditions = #condition{nexo = eq,
                                op1 = #key{alias = <<"uno">>,name = <<"uno">>},
                                op2 = #value{value = 1}}}
    ),
    ?assertEqual(sql92:parse("select * from tabla where uno=1 and dos<2"),
        #select{
    params = [#all{}],
    tables = [#table{name = <<"tabla">>,alias = <<"tabla">>}],
    conditions = 
        #condition{
            nexo = nexo_and,
            op1 = 
                #condition{
                    nexo = eq,
                    op1 = 
                        #key{alias = <<"uno">>,name = <<"uno">>},
                    op2 = #value{value = 1}},
            op2 = 
                #condition{
                    nexo = lt,
                    op1 = 
                        #key{alias = <<"dos">>,name = <<"dos">>},
                    op2 = #value{value = 2}}}}
    ),
    ?assertEqual(sql92:parse("select * from tabla where uno=1 and dos<2 and tres>3"),
        #select{
    params = [#all{}],
    tables = [#table{name = <<"tabla">>,alias = <<"tabla">>}],
    conditions = 
        #condition{
            nexo = nexo_and,
            op1 = 
                #condition{
                    nexo = eq,
                    op1 = 
                        #key{alias = <<"uno">>,name = <<"uno">>},
                    op2 = #value{value = 1}},
            op2 = 
                #condition{
                    nexo = nexo_and,
                    op1 = 
                        #condition{
                            nexo = lt,
                            op1 = 
                                #key{alias = <<"dos">>,name = <<"dos">>},
                            op2 = #value{value = 2}},
                    op2 = 
                        #condition{
                            nexo = gt,
                            op1 = 
                                #key{alias = <<"tres">>,name = <<"tres">>},
                            op2 = #value{value = 3}}}}}
    ),
    ?assertEqual(
        sql92:parse("select * from tabla where uno=1 and dos<=2 and tres>=3"),
        sql92:parse("select * from tabla where uno=1 and (dos=<2 and tres=>3)")
    ),
    ?assertEqual(
        sql92:parse("select * from tabla where tres<>3"),
        sql92:parse("select * from tabla where tres!=3")
    ),
    ?assertEqual(
        sql92:parse("select * from a where (a=1 and b=2) and c=3"),
        #select{
    params = [#all{}],
    tables = [#table{name = <<"a">>,alias = <<"a">>}],
    conditions = 
        #condition{
            nexo = nexo_and,
            op1 = 
                #condition{
                    nexo = nexo_and,
                    op1 = 
                        #condition{
                            nexo = eq,
                            op1 = #key{alias = <<"a">>,name = <<"a">>},
                            op2 = #value{value = 1}},
                    op2 = 
                        #condition{
                            nexo = eq,
                            op1 = #key{alias = <<"b">>,name = <<"b">>},
                            op2 = #value{value = 2}}},
            op2 = 
                #condition{
                    nexo = eq,
                    op1 = #key{alias = <<"c">>,name = <<"c">>},
                    op2 = #value{value = 3}}}}
    ),
    ok.
    
select_function(_) ->
    #select{params = [#function{name = <<"cast">>, params=[#value{value = <<"test plain returns">>}, {<<"CHAR">>, 60}]}]} 
      = sql92:parse("SELECT CAST('test plain returns' AS CHAR(60)) AS anon_1"),

    #select{params = [#function{name = <<"cast">>, params=[#value{value = <<"test collated returns">>}, <<"CHAR">>]}]} 
      = sql92:parse("SELECT CAST('test collated returns' AS CHAR CHARACTER SET utf8) COLLATE utf8_bin AS anon_1"),

    #select{params = [#function{name = <<"database">>, params=[]}]} 
      = sql92:parse("select database()"),
    ?assertEqual(sql92:parse("select count(*)"), 
        #select{params = [#function{name = <<"count">>, params = [#all{}]}]}
    ),
    ?assertEqual(sql92:parse("select concat('hola', 'mundo')"), 
        #select{params = [#function{name = <<"concat">>,
                            params = [#value{value = <<"hola">>},
                                      #value{value = <<"mundo">>}]}]}
    ),
    ok.

select_groupby(_) ->
    ?assertEqual(sql92:parse("select fecha, count(*) as total from datos group by fecha"),
        #select{params = [#key{alias = <<"fecha">>,
                       name = <<"fecha">>},
                  #function{name = <<"count">>,
                            params = [#all{}],
                            alias = <<"total">>}],
        tables = [#table{name = <<"datos">>,alias = <<"datos">>}],
        group = [<<"fecha">>]}
    ),
    ?assertEqual(sql92:parse("select fecha, count(*) from datos group by fecha"),
        #select{params = [#key{alias = <<"fecha">>,
                       name = <<"fecha">>},
                  #function{name = <<"count">>,
                            params = [#all{}],
                            alias = undefined}],
        tables = [#table{name = <<"datos">>,alias = <<"datos">>}],
        group = [<<"fecha">>]}
    ),
    ?assertEqual(sql92:parse("select * from a group by 1"),
        #select{params = [#all{}],
        tables = [#table{name = <<"a">>,alias = <<"a">>}],
        group = [1]}
    ),
    ok.

select_orderby(_) ->
    ?assertEqual(sql92:parse("select * from tabla order by 1"),
        #select{
            params=[#all{}],
            tables=[#table{alias = <<"tabla">>, name = <<"tabla">>}],
            order=[#order{key=1,sort=asc}]
        }
    ),
    ?assertEqual(sql92:parse("select * from tabla order by 1 desc"),
        #select{
            params=[#all{}],
            tables=[#table{alias = <<"tabla">>, name = <<"tabla">>}],
            order=[#order{key=1,sort=desc}]
        }
    ),
    ok.

select_limit(_) ->
    ?assertEqual(sql92:parse("select * from tabla limit 10"),
        #select{
            params=[#all{}],
            tables=[#table{alias = <<"tabla">>, name = <<"tabla">>}],
            limit=10
        }
    ),
    ?assertEqual(sql92:parse("select * from tabla limit 10 offset 5"),
        #select{
            params=[#all{}],
            tables=[#table{alias = <<"tabla">>, name = <<"tabla">>}],
            limit=10,
            offset=5
        }
    ),
    % see https://dev.mysql.com/doc/refman/5.7/en/select.html
    ?assertEqual(sql92:parse("select * from tabla limit 5, 10"),
        #select{
            params=[#all{}],
            tables=[#table{alias = <<"tabla">>, name = <<"tabla">>}],
            limit=10,
            offset=5
        }
    ),
    ok.

select_arithmetic(_) ->
    ?assertEqual(
        #select{params = [#operation{type = <<"+">>,
                             op1 = #value{value = 2},
                             op2 = #value{value = 3}}]},
        sql92:parse("select 2+3")
    ),
    ?assertEqual(
        sql92:parse("select 2+3"),
        sql92:parse("select (2+3)")
    ),
    ?assertNotEqual(
        sql92:parse("select (2+3)*4"),
        sql92:parse("select 2+3*4")
    ),
    ?assertEqual(
        #select{params = [#operation{type = <<"*">>,
                             op1 = #operation{type = <<"+">>,
                                              op1 = #value{value = 2},
                                              op2 = #value{value = 3}},
                             op2 = #value{value = 4}}]},
        sql92:parse("select (2+3)*4")
    ),
    ?assertEqual(
        #select{params = [#all{}],
        tables = [#table{name = <<"data">>,alias = <<"data">>}],
        conditions = #condition{nexo = eq,
                                op1 = #key{alias = <<"a">>,name = <<"a">>},
                                op2 = #operation{type = <<"*">>,
                                                 op1 = #key{alias = <<"b">>,name = <<"b">>},
                                                 op2 = #value{value = 3}}}},
        sql92:parse("select * from data where a = b*3")
    ),
    ok.






update_simple(_) ->
    #update{
      table=#table{name = <<"streams">>},
      set=[
        #set{key = <<"title">>, value=#value{value = <<"aaa">>}},
        #set{key = <<"comment">>, value=#value{value = <<>>}}],
      conditions=#condition{
        nexo=nexo_and,
        op1=#condition{nexo=eq, op1=#key{name = <<"name">>, table = <<"streams">>}, op2=#value{value = <<"updat2">>}},
        op2=#condition{nexo=eq, op1=#key{name = <<"server">>, table = <<"streams">>}, op2=#value{value = <<"localhost">>}}
      }
    } = sql92:parse("UPDATE `streams` SET `streams`.`title`='aaa', `streams`.`comment`='' WHERE  `streams`.`name`='updat2' AND `streams`.`server`='localhost'"),
    ?assertEqual(
        sql92:parse("update mitabla set dato=1"),
        #update{
            table=#table{alias = <<"mitabla">>, name = <<"mitabla">>},
            set=[#set{key = <<"dato">>, value=#value{value=1}}]
        }
    ),
    ?assertEqual(
        sql92:parse(" Update   mitabla SET dato  =  1    "),
        sql92:parse("UPDATE mitabla SET dato=1")
    ),
    ok.

update_multiparams(_) ->
    ?assertEqual(
        sql92:parse("update mitabla set dato1=1, dato2='bon jovi', dato3='this ain''t a love song'"),
        #update{
            table=#table{alias = <<"mitabla">>, name = <<"mitabla">>},
            set=[
                #set{key = <<"dato1">>, value=#value{value = 1}},
                #set{key = <<"dato2">>, value=#value{value = <<"bon jovi">>}},
                #set{key = <<"dato3">>, value=#value{value = <<"this ain't a love song">>}}
            ]
        }
    ),
    ok.

update_where(_) ->
    ?assertEqual(
        sql92:parse("update mitabla set dato=1 where dato=5"),
        #update{
            table=#table{alias = <<"mitabla">>, name = <<"mitabla">>},
            set=[#set{key = <<"dato">>, value=#value{value=1}}],
            conditions=#condition{
                nexo=eq, 
                op1=#key{alias = <<"dato">>, name = <<"dato">>},
                op2=#value{value=5}
            }
        }
    ),
    ok.










server_select_simple(_) ->
  {ok, LSocket} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
  {ok, ListenPort} = inet:port(LSocket),
  Client = spawn_link(fun() ->
    {ok, Sock} = nanomysql:connect("mysql://user:pass@127.0.0.1:"++integer_to_list(ListenPort)++"/dbname"),
    Query1 = "SELECT input,output FROM minute_stats WHERE source='net' AND time >= '2013-09-05' AND time < '2013-09-06'",
    {ok, {Columns1, Rows1}} = nanomysql:execute(Query1, Sock),
    [{<<"input">>,_}, {<<"output">>,_}] = Columns1,
    [
      [<<"20">>,20],
      [<<"30">>,30],
      [<<"40">>,undefined]
    ] = Rows1,
    ok
  end),
  erlang:monitor(process, Client),
  {ok, Sock} = gen_tcp:accept(LSocket),
  My0 = my_protocol:init([{socket,Sock}]),
  {ok, My1} = my_protocol:hello(42, My0),
  {ok, #request{info = #user{}}, My2} = my_protocol:next_packet(My1),
  {ok, My3} = my_protocol:ok(My2),

  {ok, #request{info = #select{} = Select}, My4} = my_protocol:next_packet(My3),
  #select{
    params = [#key{name = <<"input">>},#key{name = <<"output">>}],
    tables = [#table{name = <<"minute_stats">>}],
    conditions = #condition{nexo = nexo_and, 
      op1 = #condition{nexo = eq, op1 = #key{name = <<"source">>},op2 = #value{value = <<"net">>}},
      op2 = #condition{nexo = nexo_and,
        op1 = #condition{nexo = gte, op1 = #key{name = <<"time">>}, op2 = #value{value = <<"2013-09-05">>}},
        op2 = #condition{nexo = lt, op1 = #key{name = <<"time">>}, op2 = #value{value = <<"2013-09-06">>}}
      }
    }
  } = Select,

  ResponseFields = {
    [
      #column{name = <<"input">>, type=?TYPE_VARCHAR, length=20},
      #column{name = <<"output">>, type=?TYPE_LONG, length = 8}
    ],
    [
      [<<"20">>, 20],
      [<<"30">>, 30],
      [<<"40">>, undefined]
    ]
  },
  Response = #response{status=?STATUS_OK, info = ResponseFields},
  {ok, _My5} = my_protocol:send_or_reply(Response, My4),
  receive {'DOWN', _, _, Client, Reason} -> normal = Reason end,
  ok.




server_reject_password(_) ->
  {ok, LSocket} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
  {ok, ListenPort} = inet:port(LSocket),
  Client = spawn_link(fun() ->
    case nanomysql:connect("mysql://user:pass@127.0.0.1:"++integer_to_list(ListenPort)++"/dbname") of
      {error,{1045,<<"password rejected">>}} -> ok;
      {error, E} -> error(E);
      {ok,_} -> error(need_to_reject)
    end
  end),
  erlang:monitor(process, Client),
  {ok, Sock} = gen_tcp:accept(LSocket),
  My0 = my_protocol:init([{socket,Sock}]),
  {ok, My1} = my_protocol:hello(42, My0),
  {ok, #request{info = #user{}}, My2} = my_protocol:next_packet(My1),
  {ok, _My3} = my_protocol:error(<<"password rejected">>, My2),

  receive {'DOWN', _, _, Client, Reason} -> normal = Reason end,
  ok.



server_version(_) ->
  {ok, LSocket} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
  {ok, ListenPort} = inet:port(LSocket),
  Client = spawn_link(fun() ->
    {ok,C} = nanomysql:connect("mysql://user:pass@127.0.0.1:"++integer_to_list(ListenPort)++"/dbname"),
    Version = nanomysql:version(C),
    <<"5.5.6-myproto">> = Version
  end),
  erlang:monitor(process, Client),
  {ok, Sock} = gen_tcp:accept(LSocket),
  My0 = my_protocol:init([{socket,Sock}]),
  {ok, My1} = my_protocol:hello(42, My0),
  {ok, #request{info = #user{}}, My2} = my_protocol:next_packet(My1),
  {ok, _My3} = my_protocol:ok(My2),

  receive {'DOWN', _, _, Client, Reason} -> normal = Reason end,
  ok.



server_very_long_query(_) ->
  Value = binary:copy(<<"0123456789">>, 2177721),
  Query = iolist_to_binary(["INSERT INTO photos (data) VALUES ('", Value, "')"]),

  {ok, LSocket} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}, {reuseaddr, true}]),
  {ok, ListenPort} = inet:port(LSocket),
  Client = spawn_link(fun() ->
    {ok, Sock} = nanomysql:connect("mysql://user:pass@127.0.0.1:"++integer_to_list(ListenPort)++"/dbname"),
    nanomysql:execute(Query, Sock),
    ok
  end),
  erlang:monitor(process, Client),
  {ok, Sock} = gen_tcp:accept(LSocket),
  My0 = my_protocol:init([{socket,Sock},{parse_query,false}]),
  {ok, My1} = my_protocol:hello(42, My0),
  {ok, #request{info = #user{}}, My2} = my_protocol:next_packet(My1),
  {ok, My3} = my_protocol:ok(My2),

  {ok, #request{info = Query}, My4} = my_protocol:next_packet(My3),

  ResponseFields = {
    [#column{name = <<"id">>, type=?TYPE_LONG, length = 8}],
    [[20]]
  },
  Response = #response{status=?STATUS_OK, info = ResponseFields},
  {ok, _My5} = my_protocol:send_or_reply(Response, My4),

  % receive {'DOWN', _, _, Client, Reason} -> normal = Reason end,
  ok.

long_query_2(_) ->
  Values0 = binary:copy(<<"'test_name',">>, 500),
  Values = <<Values0/binary, "'test_name'">>,

  {ok, C} = nanomysql:connect("mysql://user:user@127.0.0.1:4406/test_db?login=init_db"),
  nanomysql:execute(<<"select * from test where name in (", Values/binary, ")">>, C),
  ok.


backslash(_) ->
  %% translate \x specials
  #select{params = [#value{
    value= <<0,$',$",8,10,13,9,26,$\\>>}]} =  sql92:parse("select '\\0\\'\\\"\\b\\n\\r\\t\\Z\\'"),
  %% single \
  #select{params = [#value{
    value= <<$\\>>}]} =  sql92:parse("select '\\'"),
  %% keep \ for other
  #select{params = [#value{
    value=  <<"a \\a\\ ">>}]} = sql92:parse("select '\a\ \\a\\ '").
