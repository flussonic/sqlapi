-module(sqlapi_ram_table).
-include("../include/sqlapi.hrl").


-export([tables/0, columns/1, create_table/2, drop_table/1]).
-export([select/2, insert/2, update/3, delete/2]).

-export([start_link/0]).
-export([init/1, handle_call/3, terminate/2]).


tables() ->
  [atom_to_binary(Name,latin1) || {Name,_} <- ets:tab2list(sql_ram_tables)].

columns(Name) ->
  try erlang:binary_to_existing_atom(Name,latin1) of
    NameAtom ->
      case ets:lookup(sql_ram_tables, NameAtom) of
        [{NameAtom,Spec}] -> Spec;
        [] -> {error, no_table}
      end
  catch
    _:_ -> {error, no_table}
  end.



create_table(TableName, TableSpec) when is_atom(TableName) ->
  gen_server:call(?MODULE, {create_table, TableName, TableSpec}).

drop_table(TableName) when is_atom(TableName) ->
  gen_server:call(?MODULE, {drop_table, TableName}).





select(Table, #sql_filter{conditions = Conditions, columns = Columns, limit = Limit} = Filter) ->
  case columns(Table) of
    {error, no_table} ->
      {error, 1036, <<"cannot select from non-existing ",Table/binary>>};
    TableColumns ->

      % ColumnIndex = lists:zipwith(fun({Col,_},I) -> {Col,I} end, TableColumns, lists:seq(1,length(TableColumns))),
      % Head = list_to_tuple([list_to_atom("$" ++ integer_to_list(proplists:get_value(Col,ColumnIndex))) || Col <- Columns]),
      % MatchSpec = sql_to_ms(Conditions, ColumnIndex),
      % Output = ['$$'],
      % MS = [{Head, MatchSpec, Output}],

      % Select = case Limit of 
      %   undefined -> ets:select(binary_to_existing_atom(Table,latin1),MS);
      %   _ -> ets:select(binary_to_existing_atom(Table,latin1),MS,Limit)
      % end,
      % Select1 = case Select of
      %   {error, E} ->
      %     {error, 500, <<"Failed to select: ",(atom_to_binary(E,latin1))>>};
      %   '$end_of_table' -> [];
      %   {Selection0, _Continuation0} -> Selection0;
      %   Selection0 when is_list(Selection0) -> Selection0
      % end,
      % if
      %   is_list(Select1) -> 
      %     NamedRows = [ maps:from_list(
      %       lists:zipwith(fun({Col,_},V) -> {Col,V} end, TableColumns,Row)
      %     ) || Row <- Select1],
      %     sqlapi:apply_sql_filter(NamedRows, Filter);
      %   true -> 
      %     Select1
      % end


      Rows = [lists:zipwith(fun({C,_},E) -> {C,E} end,TableColumns,tuple_to_list(R)) || R <- 
        ets:tab2list(binary_to_existing_atom(Table,latin1))],
      Reply = sqlapi:apply_sql_filter(Rows, Filter),
      Reply
  end.


% sql_to_ms(undefined, _) -> [];
% sql_to_ms(Conditions, Map) -> [sql_to_ms0(Conditions,Map)].


% sql_to_ms0({condition, Op, Arg1, Arg2}, Map) ->
%   Op1 = case Op of
%     nexo_and -> 'and';
%     nexo_or -> 'or';
%     eq -> '==';
%     lt -> '<';
%     gt -> '>';
%     lte -> '=<';
%     gte -> '>='
%   end,
%   {Op1, sql_to_ms0(Arg1, Map), sql_to_ms0(Arg2, Map)};

% sql_to_ms0({key, Key, _, _}, Map) when is_atom(Key) ->
%   proplists:get_value(Key, Map);

% sql_to_ms0({key, Key, _, _}, Map) ->
%   proplists:get_value(erlang:binary_to_existing_atom(Key,latin1), Map);

% sql_to_ms0({value, _, Arg}, _) -> Arg.









insert(Table, Rows) ->
  case columns(Table) of
    {error, no_table} ->
      {error, 1036, <<"cannot insert into non-existing ",Table/binary>>};
    Columns ->
      Tuples = lists:map(fun(R) -> list_to_tuple([maps:get(atom_to_binary(C,latin1),R,undefined) || {C,_} <- Columns]) end, Rows),
      ets:insert(binary_to_existing_atom(Table,latin1), Tuples),
      {ok, #{status => ok, affected_rows => length(Tuples)}}
  end.



update(Table, Values, Conditions) when is_map(Values) ->
  update(Table, maps:to_list(Values), Conditions);

update(Table, Values, Conditions) ->
  case columns(Table) of
    {error, no_table} ->
      {error, 1036, <<"cannot update non-existing ",Table/binary>>};
    [{Key,_}|_] = Columns ->
      UpdateIndex = lists:zipwith(fun({C,_},I) -> {atom_to_binary(C,latin1),I} end, 
        Columns, lists:seq(1,length(Columns))),
      Update = [{element(2,lists:keyfind(K,1,UpdateIndex)),V} || {K,V} <- Values],

      case select(Table, #sql_filter{conditions = Conditions, table_columns = Columns}) of
        {error, _, _} ->
          {error, 1036, <<"cannot update non-existing ",Table/binary>>};
        Rows ->
          TableAtom = binary_to_existing_atom(Table,latin1),
          [ets:update_element(TableAtom,maps:get(Key,R),Update) || R <- Rows],
          {ok, #{status => ok, affected_rows => length(Rows)}}
      end
  end.



delete(Table, Conditions) ->
  case columns(Table) of
    {error, no_table} ->
      {error, 1036, <<"cannot delete from non-existing ",Table/binary>>};
    [{Key,_}|_] = Columns ->
      case select(Table, #sql_filter{conditions = Conditions, table_columns = Columns}) of
        {error, _, _} ->
          {error, 1036, <<"cannot delete from non-existing ",Table/binary>>};
        Rows ->
          TableAtom = binary_to_existing_atom(Table,latin1),
          [ets:delete(TableAtom,maps:get(Key,R)) || R <- Rows],
          {ok, #{affected_rows => length(Rows), status => ok}}
      end
  end.




start_link() ->
  gen_server:start_link({local,?MODULE}, ?MODULE, [], []).

init([]) ->
  ets:new(sql_ram_tables, [public,named_table]),
  {ok, state}.


handle_call({create_table, TableName, TableSpec}, _From, State) ->
  case ets:lookup(sql_ram_tables, TableName) of
    [] ->
      try ets:new(TableName, [public,named_table]) of
        TableName ->
          ets:insert(sql_ram_tables, {TableName, TableSpec}),
          {reply, ok, State}
      catch
        _:_ ->
          {reply, {error, badarg}, State}
      end;
    [_] ->
      {reply, {error, exists}, State}
  end;

handle_call({drop_table, TableName}, _From, State) ->
  case ets:lookup(sql_ram_tables, TableName) of
    [] ->
      {reply, {error, enoent}, State};
    [_] ->
      ets:delete(sql_ram_tables, TableName),
      ets:delete(TableName),
      {reply, ok, State}
  end.



terminate(_,_) -> ok.






