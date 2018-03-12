-module(sql_ets_api).

-export([authorize/4, connect_db/2, database/1, databases/1, tables/1, columns/2, terminate/2]).
-export([select/3, insert/3, update/4, delete/3, fncall/3]).


-record(state, {
  db
}).

authorize(Username, HashedPassword, Hash, _Env) ->
  case <<"login">> == Username andalso sqlapi:password_hash(<<"pass">>, Hash) == HashedPassword of
    true -> {ok, #state{}};
    false -> {error, <<"Invalid login or password">>}
  end.


connect_db(Database,State) ->
  {ok, State#state{db=Database}}.

database(#state{db=DB}) -> DB.

databases(#state{}) -> [<<"ets">>].


tables(#state{}) -> sqlapi_ram_table:tables().

columns(Table, #state{}) -> sqlapi_ram_table:columns(Table).

terminate(_,_State) -> ok.

select(Table, Filter, _State) -> sqlapi_ram_table:select(Table, Filter).

insert(Table, Values, _State) -> sqlapi_ram_table:insert(Table, Values).

update(Table, Values, Conditions, _State) -> sqlapi_ram_table:update(Table, Values, Conditions).

delete(Table, Conditions, _State) -> sqlapi_ram_table:delete(Table, Conditions).

% SELECT create_ram_table('sessions','session_id:string','user_id:integer','last_seen_at:integer');
fncall(<<"create_ram_table">>, [TableName | ColumnList], _State) ->
  Columns = lists:map(fun(C) ->
    [Column, Type] = binary:split(C,<<":">>),
    {binary_to_atom(Column,latin1),binary_to_atom(Type,latin1)}
  end, ColumnList),
  Table = binary_to_atom(TableName,latin1),
  sqlapi_ram_table:create_table(Table, Columns),
  {ok, #{status => ok}};

fncall(<<"drop_ram_table">>, [TableName], _State) ->
  sqlapi_ram_table:drop_table(binary_to_atom(TableName,latin1)),
  {ok, #{status => ok}}.

