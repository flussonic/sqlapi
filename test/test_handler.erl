-module (test_handler).
-include("../src/myproto.hrl").



-export([authorize/4,connect_db/2, database/1, databases/1, tables/1, columns/2, version/1, terminate/2]).
-export([select/3, insert/3, update/4, delete/3, fncall/3]).




-define(ERR_WRONG_PASS, {error, <<"Password incorrect!">>}).
-define(ERR_WRONG_USER, {error, <<"No such user!">>}).
-define(ERR_LOGIN_DISABLED, {error, <<"Login disabled">>}).
-define(ERR_INFO(Code, Desc), #response{status=?STATUS_ERR, error_code=Code, info=Desc}).


-record(my, {
  db
}).

authorize(<<"user">>, _Pass, _Hash, _) -> {ok, #my{}};
authorize(_,_,_,_) -> {error, <<"Bad user">>}.


connect_db(DB, My) -> {ok, My#my{db=DB}}.
database(#my{db=DB}) -> DB.
version(_) -> <<"5.6.0">>.
databases(_) -> [<<"test_db">>].
tables(_) -> [<<"test">>].
columns(<<"test">>,_) -> [{id,string},{name,string},{url,string}];
columns(Table, _) -> sqlapi_ram_table:columns(Table).



fncall(_,_,_) -> error(not_implemented).
insert(Table,Values,_) -> sqlapi_ram_table:insert(Table, Values).
delete(Table,Conditions,_) -> sqlapi_ram_table:delete(Table, Conditions).
update(Table,Values,Conditions,_) -> sqlapi_ram_table:update(Table, Values, Conditions).


select(<<"test">>, Filter, #my{}) ->
  Rows = [ [{id, <<"1">>}, {name, <<"stream1">>}, {url, <<"rtsp://...">>}] ],
  sqlapi:apply_sql_filter(Rows, Filter);
select(Table,Conditions,_) -> sqlapi_ram_table:select(Table, Conditions).




terminate(_Reason,_) -> 
  ok.
